import boto3
import os
import uuid
import json
from datetime import datetime, timezone
from decimal import Decimal

PEDIDO_TABLE = os.environ.get('PEDIDO_TABLE')
CONNECTIONS_TABLE = os.environ.get('CONNECTIONS_TABLE')
dynamodb = boto3.resource('dynamodb')

def convert_floats_to_decimal(obj):
    """Convierte recursivamente todos los floats a Decimal para DynamoDB"""
    if isinstance(obj, list):
        return [convert_floats_to_decimal(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_floats_to_decimal(value) for key, value in obj.items()}
    elif isinstance(obj, float):
        return Decimal(str(obj))
    else:
        return obj

def convert_decimal_to_float(obj):
    """Convierte recursivamente todos los Decimal a float para JSON serialization"""
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
        response = connections_table.scan(ProjectionExpression='connectionId')
        connections = response.get('Items', [])
    except Exception as e:
        print(f"[Error Transmitir] Fallo al escanear la tabla de conexiones: {e}")
        return

    print(f"Encontradas {len(connections)} conexiones para transmitir.")
    
    message_payload_str = json.dumps(message_payload_dict)

    # Enviar a TODAS las conexiones activas
    for connection in connections:
        connection_id = connection['connectionId']
        
        try:
            apigateway_client.post_to_connection(
                ConnectionId=connection_id,
                Data=message_payload_str.encode('utf-8')
            )
            print(f"[Info Transmitir] Mensaje enviado a {connection_id}")
        except apigateway_client.exceptions.GoneException:
            print(f"[Info Transmitir] Conexión muerta {connection_id}. Limpiando.")
            connections_table.delete_item(Key={'connectionId': connection_id})
        except Exception as e:
            print(f"[Error Transmitir] No se pudo enviar a {connection_id}: {e}")

def connection_manager(event, context):
    connection_id = event['requestContext']['connectionId']
    route_key = event['requestContext']['routeKey']

    if not CONNECTIONS_TABLE:
        print("Error: CONNECTIONS_TABLE no está definida en las variables de entorno.")
        return {'statusCode': 500, 'body': 'Error de configuración del servidor.'}
        
    table = dynamodb.Table(CONNECTIONS_TABLE)

    if route_key == '$connect':
        try:
            
            item = {
                'connectionId': connection_id,
                'Role': "CLIENTE",
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

def publishPedido(event, context):
    print(f"publishPedido invocado. Evento: {event}")
    connection_id = event['requestContext']['connectionId']
    connections_table = dynamodb.Table(CONNECTIONS_TABLE)

    # Verificar conexión autorizada
    conn_resp = connections_table.get_item(Key={'connectionId': connection_id})
    if 'Item' not in conn_resp:
        return {'statusCode': 403, 'body': json.dumps('Conexión no autorizada. Reconecte.')}
    
    try:
        body = json.loads(event.get('body', '{}'))
        
        pedido = {
            "tenant_id": body.get('tenant_id'),  # usuario - HASH key
            "uuid": body.get('uuid', str(uuid.uuid4())),  # RANGE key - Generar UUID si no se proporciona
            "fecha_pedido": body.get('fecha_pedido'),
            "fecha_entrega": body.get('fecha_entrega'),
            "estado_pedido": body.get('estado_pedido'),
            "multiplicador_de_puntos": Decimal(str(body.get('multiplicador_de_puntos', 1.0))),
            "delivery": body.get('delivery', False),
            "beneficios": body.get('beneficios', []),
            "elementos": []
        }
        
        elementos = body.get('elementos', [])
        for elemento in elementos:
            elemento_procesado = {
                "combo": elemento.get('combo', []),
                "cantidad_combo": elemento.get('cantidad_combo', ""),
                "productos": {
                    "hamburguesa": [],
                    "papas": [],
                    "complementos": [],
                    "adicionales": []
                }
            }
            
            productos = elemento.get('productos', {})
            
            hamburguesas = productos.get('hamburguesa', [])
            for hamburguesa in hamburguesas:
                hamburguesa_procesada = {
                    "nombre": hamburguesa.get('nombre'),
                    "ingredientes": hamburguesa.get('ingredientes', []),
                    "tamaño": hamburguesa.get('tamaño'),
                    "extra": hamburguesa.get('extra')
                }
                elemento_procesado["productos"]["hamburguesa"].append(hamburguesa_procesada)
            
            elemento_procesado["productos"]["papas"] = productos.get('papas', [])

            elemento_procesado["productos"]["complementos"] = productos.get('complementos', [])
            
            elemento_procesado["productos"]["adicionales"] = productos.get('adicionales', [])
            
            precio_base = Decimal(str(elemento.get('precio_base_combo', 0)))
            porcentaje = Decimal(str(elemento.get('porcentaje', 1.0)))
            extra_producto = Decimal(str(elemento.get('extra_producto_modificado', 0)))
            elemento_procesado["precio"] = (precio_base * porcentaje) + extra_producto
            
            puntos_base = Decimal(str(elemento.get('puntos_base', 0)))
            elemento_procesado["puntos"] = puntos_base * Decimal(str(pedido["multiplicador_de_puntos"]))
            
            pedido["elementos"].append(elemento_procesado)
        
        # Guardar pedido en DynamoDB (convertir floats a Decimal)
        try:        
            dbPedido = dynamodb.Table(PEDIDO_TABLE)
            pedido_for_db = convert_floats_to_decimal(pedido)
            dbPedido.put_item(Item=pedido_for_db)
            print(f"Pedido guardado exitosamente: tenant_id={pedido['tenant_id']}, uuid={pedido['uuid']}")
        except Exception as e:
            print(f"Error guardando pedido en DynamoDB: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps('Error guardando pedido en base de datos')
            }

        # Crear payload para transmisión (convertir Decimal a float para JSON)
        pedido_for_transmission = convert_decimal_to_float(pedido)
        message_payload = {
            "action": "nuevo_pedido",
            "pedido": pedido_for_transmission,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        # Transmitir pedido a TODAS las conexiones activas
        transmitir(event, message_payload)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                "message": "Pedido procesado y transmitido exitosamente",
                "pedido_uuid": pedido["uuid"]
            })
        }

    except json.JSONDecodeError:
        return {'statusCode': 400, 'body': json.dumps('JSON inválido en el cuerpo de la solicitud')}
    except KeyError as e:
        return {'statusCode': 400, 'body': json.dumps(f'Campo requerido faltante: {str(e)}')}
    except Exception as e:
        print(f"Error procesando pedido: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error interno del servidor procesando el pedido')
        }

def pedidoFiltro(event, context):
    print(f"pedidoFiltro invocado. Evento: {event}")
    
    # Esta es una petición HTTP GET, no requiere verificación de conexión WebSocket
    try:
        # Obtener parámetros de query string para los filtros
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
