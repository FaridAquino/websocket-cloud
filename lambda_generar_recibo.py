import json
import boto3
import os
from datetime import datetime, timezone
from decimal import Decimal

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def lambda_handler(event, context):
    print(f"[Generar Recibo] Evento recibido: {json.dumps(event)}")
    
    # Variables de entorno
    PEDIDO_TABLE = os.environ.get('PEDIDO_TABLE')
    S3_BUCKET = os.environ.get('S3_BUCKET')
    
    if not PEDIDO_TABLE or not S3_BUCKET:
        print("[Error] Faltan variables de entorno PEDIDO_TABLE o S3_BUCKET")
        return {'statusCode': 500, 'body': 'Error de configuración'}
    
    dynamodb = boto3.resource('dynamodb')
    s3 = boto3.client('s3')
    
    try:
        # Procesar mensaje de SNS
        for record in event['Records']:
            sns_message = json.loads(record['Sns']['Message'])
            
            tenant_id = sns_message.get('tenant_id')
            uuid_pedido = sns_message.get('uuid')
            cliente_email = sns_message.get('cliente_email')
            
            if not tenant_id or not uuid_pedido or not cliente_email:
                print(f"[Error] Datos incompletos en SNS: {sns_message}")
                continue
            
            # Obtener datos completos del pedido desde DynamoDB
            table = dynamodb.Table(PEDIDO_TABLE)
            response = table.get_item(
                Key={
                    'tenant_id': tenant_id,
                    'uuid': uuid_pedido
                }
            )
            
            if 'Item' not in response:
                print(f"[Error] Pedido no encontrado: {tenant_id}/{uuid_pedido}")
                continue
            
            pedido = response['Item']
            
            # Generar contenido del recibo
            recibo_content = generar_recibo_txt(pedido, sns_message.get('payment_id'))
            
            # Nombre del archivo: cliente_email.txt
            file_name = f"{cliente_email}.txt"
            
            # Subir recibo a S3
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=f"recibos/{file_name}",
                Body=recibo_content.encode('utf-8'),
                ContentType='text/plain'
            )
            
            print(f"[Success] Recibo generado y subido a S3: recibos/{file_name}")
    
    except Exception as e:
        print(f"[Error] Error generando recibo: {str(e)}")
        return {'statusCode': 500, 'body': f'Error: {str(e)}'}
    
    return {'statusCode': 200, 'body': 'Recibo generado exitosamente'}

def generar_recibo_txt(pedido, payment_id):
    """Genera el contenido del recibo en formato texto"""
    
    # Convertir Decimals para cálculos
    pedido_clean = json.loads(json.dumps(pedido, default=decimal_default))
    
    # Información básica
    fecha_actual = datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M:%S UTC")
    tenant_id = pedido_clean.get('tenant_id', 'N/A')
    uuid_pedido = pedido_clean.get('uuid', 'N/A')
    cliente_email = pedido_clean.get('cliente_email', 'N/A')
    cliente_nombre = pedido_clean.get('cliente_nombre', 'Cliente')
    preference_id = pedido_clean.get('preference_id', 'N/A')
    
    # Elementos del pedido
    elementos = pedido_clean.get('elementos', [])
    
    # Calcular totales
    total = 0.0
    detalle_items = []
    
    for item in elementos:
        precio = float(item.get('precio', 0))
        cantidad = int(item.get('cantidad_combo', 1))
        combo = item.get('combo', ['Producto'])[0]
        subtotal = precio * cantidad
        total += subtotal
        
        detalle_items.append(f"  {combo}")
        detalle_items.append(f"    Cantidad: {cantidad}")
        detalle_items.append(f"    Precio unitario: S/. {precio:.2f}")
        detalle_items.append(f"    Subtotal: S/. {subtotal:.2f}")
        detalle_items.append("")
    
    # Construir el recibo
    recibo = f"""
========================================
           RECIBO DE PAGO - BEMBOS
========================================

Fecha de generación: {fecha_actual}
ID de Pedido: {uuid_pedido}
Tenant ID: {tenant_id}
ID de Pago MercadoPago: {payment_id}
ID de Preferencia: {preference_id}

----------------------------------------
           DATOS DEL CLIENTE
----------------------------------------
Nombre: {cliente_nombre}
Email: {cliente_email}

----------------------------------------
           DETALLE DEL PEDIDO
----------------------------------------
{chr(10).join(detalle_items)}
----------------------------------------
           RESUMEN DE PAGO
----------------------------------------
Total pagado: S/. {total:.2f}
Estado: PAGADO
Método de pago: MercadoPago

----------------------------------------
¡Gracias por tu compra!

Este recibo confirma que tu pago ha sido
procesado exitosamente. Tu pedido está
siendo preparado.

Para consultas: soporte@bembos.com
========================================
"""
    
    return recibo.strip()