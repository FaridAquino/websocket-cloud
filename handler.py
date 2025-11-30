import json
import os
import random
import boto3
from decimal import Decimal
from datetime import datetime, timezone
import mercadopago
import uuid

PEDIDO_TABLE = os.environ.get('PEDIDO_TABLE')
CONNECTIONS_TABLE = os.environ.get('CONNECTIONS_TABLE')
dynamodb = boto3.resource('dynamodb')

def convert_floats_to_decimal(obj):
    if isinstance(obj, list):
        return [convert_floats_to_decimal(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_floats_to_decimal(value) for key, value in obj.items()}
    elif isinstance(obj, float):
        return Decimal(str(obj))
    else:
        return obj

def convert_decimal_to_float(obj):
    if isinstance(obj, list):
        return [convert_decimal_to_float(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_decimal_to_float(value) for key, value in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj 

def transmitir(event, message_payload_dict):
    try:
        connections_table = dynamodb.Table(CONNECTIONS_TABLE)
    except Exception as e:
        print(f"[Error Transmitir] No se pudieron cargar las tablas: {e}")
        return
    
    try:
        endpoint_url = f"https://{event['requestContext']['domainName']}/{event['requestContext']['stage']}"
        apigateway_client = boto3.client('apigatewaymanagementapi', endpoint_url=endpoint_url)
    except KeyError:
        print(f"[Error Transmitir] El evento no tiene 'requestContext' para el endpoint_url.")
        return

    pedido_data = message_payload_dict.get('pedido', {})
    
    if not pedido_data:
        print("[Error Transmitir] No se encontró el objeto 'pedido' en el payload.")
        return

    try:
        response = connections_table.scan(ProjectionExpression='connectionId, #r',ExpressionAttributeNames={'#r': 'role'})
        connections = response.get('Items', [])
    except Exception as e:
        print(f"[Error Transmitir] Fallo al escanear la tabla de conexiones: {e}")
        return

    print(f"Encontradas {len(connections)} conexiones para evaluar.")
    
    message_payload_str = json.dumps(message_payload_dict)
    chefs_found = 0

    for connection in connections:
        connection_id = connection['connectionId']
        user_role = connection.get('role', 'CLIENTE')
        
        if user_role == 'CHEF':
            chefs_found += 1
            try:
                apigateway_client.post_to_connection(
                    ConnectionId=connection_id,
                    Data=message_payload_str.encode('utf-8')
                )
                print(f"[Info Transmitir] Pedido enviado a chef: {connection_id}")
            except apigateway_client.exceptions.GoneException:
                print(f"[Info Transmitir] Conexión de chef muerta {connection_id}. Limpiando.")
                connections_table.delete_item(Key={'connectionId': connection_id})
            except Exception as e:
                print(f"[Error Transmitir] No se pudo enviar a chef {connection_id}: {e}")
        else:
            print(f"[Info Transmitir] Saltando conexión {connection_id} - Rol: {user_role} (no es chef)")
    
    print(f"[Info Transmitir] Pedido transmitido a {chefs_found} chefs conectados.")

def connection_manager(event, context):
    connection_id = event['requestContext']['connectionId']
    route_key = event['requestContext']['routeKey']

    query_params = event.get('queryStringParameters', {}) or {}

    if not CONNECTIONS_TABLE:
        print("Error: CONNECTIONS_TABLE no está definida en las variables de entorno.")
        return {'statusCode': 500, 'body': 'Error de configuración del servidor.'}
        
    table = dynamodb.Table(CONNECTIONS_TABLE)

    if route_key == '$connect':
        try:
            
            item = {
                'connectionId': connection_id,
                'role': query_params.get('role', 'CLIENTE'),
                'connectTime': datetime.now(timezone.utc).isoformat()
            }

            table.put_item(Item=item)
            
            return {'statusCode': 200, 'body': 'Conectado.'}

        except Exception as e:
            print(f"Error en $connect: {e}")
            return {'statusCode': 500, 'body': 'Fallo en $connect.'}

    elif route_key == '$disconnect':
        try:
            table.delete_item(
                Key={'connectionId': connection_id}
            )
            print(f"Conexión eliminada: {connection_id}")
            
            return {'statusCode': 200, 'body': 'Desconectado.'}
            
        except Exception as e:
            print(f"Error en $disconnect (no crítico): {e}")
            return {'statusCode': 200, 'body': 'Desconectado con error de limpieza.'}

    return {'statusCode': 500, 'body': 'Error en connection_manager.'}

def default_handler(event, context):
    print(f"Ruta $default invocada. Evento: {event}")
    return {
        'statusCode': 404,
        'body': json.dumps("Acción no reconocida.")
    }

def pedidoFiltro(event, context):
    print(f"pedidoFiltro invocado. Evento: {event}")
    
    try:
        query_params = event.get('queryStringParameters', {}) or {}
        
        # Parámetros de filtro
        fecha_pedido = query_params.get('fecha_pedido')
        estado_pedido = query_params.get('estado_pedido')
        fecha_entrega = query_params.get('fecha_entrega')
        
        # Construir el filtro de DynamoDB
        filter_expression = None
        expression_attribute_names = {}
        expression_attribute_values = {}
        
        conditions = []
        
        # Filtro por fecha_pedido
        if fecha_pedido:
            conditions.append('#fecha_pedido = :fecha_pedido')
            expression_attribute_names['#fecha_pedido'] = 'fecha_pedido'
            expression_attribute_values[':fecha_pedido'] = fecha_pedido
        
        # Filtro por estado_pedido
        if estado_pedido:
            conditions.append('#estado_pedido = :estado_pedido')
            expression_attribute_names['#estado_pedido'] = 'estado_pedido'
            expression_attribute_values[':estado_pedido'] = estado_pedido
        
        # Filtro por fecha_entrega
        if fecha_entrega:
            conditions.append('#fecha_entrega = :fecha_entrega')
            expression_attribute_names['#fecha_entrega'] = 'fecha_entrega'
            expression_attribute_values[':fecha_entrega'] = fecha_entrega
        
        # Combinar condiciones con AND
        if conditions:
            filter_expression = ' AND '.join(conditions)
        
        pedidos_table = dynamodb.Table(PEDIDO_TABLE)
        
        scan_kwargs = {}
        if filter_expression:
            scan_kwargs['FilterExpression'] = filter_expression
            scan_kwargs['ExpressionAttributeNames'] = expression_attribute_names
            scan_kwargs['ExpressionAttributeValues'] = expression_attribute_values
        
        response = pedidos_table.scan(**scan_kwargs)
        pedidos_filtrados = response.get('Items', [])
        
        while 'LastEvaluatedKey' in response:
            scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
            response = pedidos_table.scan(**scan_kwargs)
            pedidos_filtrados.extend(response.get('Items', []))
        
        # Convertir Decimals a floats para respuesta JSON
        pedidos_for_response = convert_decimal_to_float(pedidos_filtrados)
        
        print(f"Pedidos filtrados encontrados: {len(pedidos_filtrados)}")
        
        # Devolver resultados directamente en la respuesta HTTP (no transmitir por WebSocket)
        return {
            'statusCode': 200,
            'body': json.dumps({
                "message": "Filtro de pedidos aplicado exitosamente",
                "total_encontrados": len(pedidos_filtrados),
                "filtros_aplicados": {
                    "fecha_pedido": fecha_pedido,
                    "estado_pedido": estado_pedido,
                    "fecha_entrega": fecha_entrega
                },
                "pedidos": pedidos_for_response,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
        }
        
    except Exception as e:
        print(f"Error procesando filtro de pedido: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error interno del servidor procesando el filtro: {str(e)}')
        }

def pagarPedido(event, context):
    
    ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
    
    if not ACCESS_TOKEN:
        return {
            'statusCode': 500,
            'body': json.dumps("Error del Servidor: Falta configuración (ACCESS_TOKEN)")
        }

    sdk = mercadopago.SDK(ACCESS_TOKEN)
    
    try:
        rc = event.get('requestContext', {})
        domain_name = rc.get('domainName')
        stage = rc.get('stage')

        if not domain_name:
            return {"statusCode": 500, "body": "Error: No domainName"}

        if stage == '$default':
            webhook_url = f"https://{domain_name}/webhook"
        else:
            webhook_url = f"https://{domain_name}/{stage}/webhook"
            
        print(f"Webhook URL generada: {webhook_url}")

        body_str = event.get('body', '{}')
        body = json.loads(body_str) if isinstance(body_str, str) else body_str

        print(f"Payload del Pedido: {json.dumps(body)}")

        # 3. LÓGICA DE CÁLCULO DE PRECIO
        total_a_cobrar = 0.0
        items_mp = []
        
        elementos = body.get("elementos", [])

        if not elementos:
            return {
                'statusCode': 400,
                'body': json.dumps("Error: No se proporcionaron elementos en el pedido.")
            }
        
        for item in elementos:
            precio = float(item.get("precio"))
            cantidad = int(item.get("cantidad_combo"))
            total_a_cobrar += precio * cantidad
            
            nombre_combo = item.get("combo", ["Combo"])[0]
            items_mp.append({
                "id": str(random.randint(1000, 9999)),
                "title": nombre_combo,
                "description": "Pedido de combo desde aplicación",
                "picture_url": body.get("imagen_combo_url", "https://images.wondershare.com/repairit/article/error-image-error-loading-2.jpg"),
                "quantity": cantidad,
                "currency_id": "PEN",
                "unit_price": precio
            })

        if total_a_cobrar==0:
            return {
                'statusCode': 400,
                'body': json.dumps("Error: El total a cobrar no puede ser cero.")
            }
            
        print(f"Total a Cobrar: S/. {total_a_cobrar}")

        # 4. PREPARAR REFERENCIA EXTERNA (CRÍTICO PARA EL WEBHOOK)
        # Enviamos un JSON string con los IDs necesarios para ubicar el pedido en DynamoDB
        tenant_id = body.get("tenant_id", "sin_tenant")
        uuid_val = body.get("uuid")
        
        ref_data = {
            "tenant_id": tenant_id,
            "uuid": str(uuid_val)
        }
        external_ref_json = json.dumps(ref_data)

        # 5. PREPARAR DATOS MP
        back_urls = {
            "success": "https://www.google.com/search?q=pago_exitoso",
            "failure": "https://www.google.com/search?q=pago_fallido",
            "pending": "https://www.google.com/search?q=pago_pendiente"
        }
        
        
        client_email=body.get("cliente_email")

        preference_data = {
            "items": items_mp,
            "payer": {
                "email": client_email,
            },
            "external_reference": external_ref_json,
            "payment_methods": {
                "excluded_payment_methods": [{"id": "visa"}],
                "installments": 6
            },
            "back_urls": back_urls,
            "auto_return": "approved",
            "notification_url": webhook_url, 
        }

        preference_response = sdk.preference().create(preference_data)
        mp_response = preference_response["response"]
        
        if preference_response["status"] not in [200, 201]:
            return {
                'statusCode': 400,
                'body': json.dumps(mp_response)
            }

        print(f"Preferencia creada en MP: {json.dumps(mp_response)}")

        sandbox_init_point = mp_response['sandbox_init_point']
        preference_id = mp_response['id']

        if PEDIDO_TABLE:
            try:
                table = dynamodb.Table(PEDIDO_TABLE)
                
                # Validar si ya existe el pedido con el mismo tenant_id y uuid
                try:
                    existing_item = table.get_item(
                        Key={
                            'tenant_id': tenant_id,
                            'uuid': str(uuid_val)
                        }
                    )
                    
                    if 'Item' in existing_item:
                        print(f"[Warning] Pedido ya existe: tenant_id={tenant_id}, uuid={str(uuid_val)}")
                        # Actualizar solo el preference_id y fecha si ya existe
                        table.update_item(
                            Key={
                                'tenant_id': tenant_id,
                                'uuid': str(uuid_val)
                            },
                            UpdateExpression="set preference_id = :pref_id, fecha_creacion = :fecha",
                            ExpressionAttributeValues={
                                ':pref_id': preference_id,
                                ':fecha': datetime.now(timezone.utc).isoformat()
                            }
                        )
                        print(f"Pedido actualizado con nueva preferencia: {preference_id}")
                    else:
                        # No existe, crear nuevo item
                        item_db = json.loads(json.dumps(body), parse_float=Decimal)
                        
                        item_db["estado_pedido"] = "PENDIENTE_PAGO"
                        item_db["preference_id"] = preference_id
                        item_db["fecha_creacion"] = datetime.now(timezone.utc).isoformat()
                        item_db["uuid"] = str(uuid_val)

                        table.put_item(Item=item_db)
                        print(f"Pedido guardado en DynamoDB (PENDIENTE_PAGO): {str(uuid_val)}")
                        
                except Exception as db_error:
                    print(f"Error en operación DynamoDB: {str(db_error)}")
                    
            except Exception as e:
                print(f"Error general guardando en DynamoDB: {str(e)}")

        return {
            'statusCode': 200,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({
                "message": "Link generado",
                "sandbox_init_point": sandbox_init_point,
                "preference_id": preference_id
            })
        }

    except Exception as e:
        print(f"Error Crítico: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error interno: {str(e)}")
        }

def obtenerPedidosPorEmail(event, context):
    print(f"obtenerPedidosPorEmail invocado. Evento: {event}")
    
    try:
        # Obtener el body del request
        body_str = event.get('body', '{}')
        body = json.loads(body_str) if isinstance(body_str, str) else body_str
        
        cliente_email = body.get('cliente_email')
        
        if not cliente_email:
            return {
                'statusCode': 400,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': 'cliente_email es requerido'})
            }
        
        # Buscar pedidos por cliente_email
        pedidos_table = dynamodb.Table(PEDIDO_TABLE)
        
        response = pedidos_table.scan(
            FilterExpression='cliente_email = :email',
            ExpressionAttributeValues={':email': cliente_email}
        )
        
        pedidos_encontrados = response.get('Items', [])
        
        # Continuar escaneando si hay más elementos
        while 'LastEvaluatedKey' in response:
            response = pedidos_table.scan(
                FilterExpression='cliente_email = :email',
                ExpressionAttributeValues={':email': cliente_email},
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            pedidos_encontrados.extend(response.get('Items', []))
        
        # Convertir Decimals a floats para respuesta JSON
        pedidos_for_response = convert_decimal_to_float(pedidos_encontrados)
        
        print(f"Pedidos encontrados para {cliente_email}: {len(pedidos_encontrados)}")
        
        return {
            'statusCode': 200,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({
                "message": "Pedidos obtenidos exitosamente",
                "cliente_email": cliente_email,
                "total_pedidos": len(pedidos_encontrados),
                "pedidos": pedidos_for_response,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
        }
        
    except Exception as e:
        print(f"Error obteniendo pedidos por email: {e}")
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'Error interno del servidor: {str(e)}'})
        }

def obtenerPedidoPorId(event, context):
    print(f"obtenerPedidoPorId invocado. Evento: {event}")
    
    try:
        # Obtener el body del request
        body_str = event.get('body', '{}')
        body = json.loads(body_str) if isinstance(body_str, str) else body_str
        
        tenant_id = body.get('tenant_id')
        uuid_pedido = body.get('uuid')
        
        if not tenant_id or not uuid_pedido:
            return {
                'statusCode': 400,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': 'tenant_id y uuid son requeridos'})
            }
        
        # Buscar pedido por clave primaria
        pedidos_table = dynamodb.Table(PEDIDO_TABLE)
        
        response = pedidos_table.get_item(
            Key={
                'tenant_id': tenant_id,
                'uuid': uuid_pedido
            }
        )
        
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({
                    'error': 'Pedido no encontrado',
                    'tenant_id': tenant_id,
                    'uuid': uuid_pedido
                })
            }
        
        pedido = response['Item']
        
        # Convertir Decimals a floats para respuesta JSON
        pedido_for_response = convert_decimal_to_float(pedido)
        
        print(f"Pedido encontrado: {tenant_id}/{uuid_pedido}")
        
        return {
            'statusCode': 200,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({
                "message": "Pedido obtenido exitosamente",
                "tenant_id": tenant_id,
                "uuid": uuid_pedido,
                "pedido": pedido_for_response,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
        }
        
    except Exception as e:
        print(f"Error obteniendo pedido por ID: {e}")
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': f'Error interno del servidor: {str(e)}'})
        }

def receiveWebhook(event, context):
    print(f"[receiveWebhook] Evento recibido: {json.dumps(event)}")

    ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
    SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

    if not ACCESS_TOKEN:
        print("[Error] Falta configuración ACCESS_TOKEN")
        return {'statusCode': 500, 'body': "Error de configuración"}

    sdk = mercadopago.SDK(ACCESS_TOKEN)
    sns = boto3.client('sns')
    
    try:
        body = {}
        if 'body' in event and event['body']:
            body_str = event['body']
            body = json.loads(body_str) if isinstance(body_str, str) else body_str

        payment_id = body.get("data", {}).get("id")
        topic = body.get("type")

        if not payment_id and event.get("queryStringParameters"):
            qs = event["queryStringParameters"]
            payment_id = qs.get("data.id") or qs.get("id")
            topic = qs.get("type") or qs.get("topic")

        if topic != "payment" and topic != "payment_created": 
            if not payment_id:
                return {'statusCode': 200, 'body': 'Ignored/OK'}

        print(f"Verificando pago ID en MP: {payment_id}")

        payment_info = sdk.payment().get(payment_id)
        
        if payment_info["status"] != 200:
            print(f"[Error MP API] Status: {payment_info['status']}")
            return {'statusCode': 200, 'body': 'Error consultando MP'}

        payment_data = payment_info["response"]
        status = payment_data["status"]
        external_ref_raw = payment_data.get("external_reference")

        print(f"Estado del pago: {status}")

        if status == "approved":
            try:
                keys = json.loads(external_ref_raw)
                tenant_id = keys.get("tenant_id")
                uuid_pedido = keys.get("uuid")
            except Exception:
                print(f"[Error] external_reference inválida: {external_ref_raw}")
                return {'statusCode': 200, 'body': 'Bad Reference'}

            if PEDIDO_TABLE and tenant_id and uuid_pedido:
                table = dynamodb.Table(PEDIDO_TABLE)
                
                try:
                    response = table.update_item(
                        Key={
                            'tenant_id': tenant_id,
                            'uuid': uuid_pedido
                        },
                        UpdateExpression="set estado_pedido=:s",
                        ExpressionAttributeValues={
                            ':s': 'PAGADO',
                        },
                        ReturnValues="ALL_NEW"
                    )
                    print(f"DynamoDB actualizado: Pedido {uuid_pedido} -> PAGADO")
                    
                    # Obtener datos del pedido actualizado
                    pedido_actualizado = response.get('Attributes', {})
                    cliente_email = pedido_actualizado.get('cliente_email')
                    cliente_nombre = pedido_actualizado.get('cliente_nombre')
                    
                    # Enviar notificación a SNS para generar recibo y enviar email
                    if SNS_TOPIC_ARN and cliente_email:
                        sns_message = {
                            'tenant_id': tenant_id,
                            'uuid': uuid_pedido,
                            'cliente_email': cliente_email,
                            'cliente_nombre': cliente_nombre or 'Cliente',
                            'payment_id': payment_id,
                            'evento': 'pedido_pagado'
                        }
                        
                        sns.publish(
                            TopicArn=SNS_TOPIC_ARN,
                            Message=json.dumps(sns_message),
                            Subject=f'Pedido Pagado - {uuid_pedido}'
                        )
                        print(f"Notificación enviada a SNS para pedido: {uuid_pedido}")
                    else:
                        print(f"[Warning] No se pudo enviar a SNS - SNS_TOPIC_ARN: {SNS_TOPIC_ARN}, cliente_email: {cliente_email}")

                except Exception as e:
                    print(f"[Error Crítico DB] {str(e)}")
                    return {'statusCode': 500, 'body': 'Error actualizando DB'}
                    
        return {'statusCode': 200, 'body': 'Webhook procesado'}

    except Exception as e:
        print(f"[Error Handler Webhook] {str(e)}")
        return {'statusCode': 200, 'body': 'Error manejado'}