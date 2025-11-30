import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
import boto3
import os
from decimal import Decimal

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def lambda_handler(event, context):
    print(f"[Enviar Email] Evento recibido: {json.dumps(event)}")
    
    # Variables de entorno
    PEDIDO_TABLE = os.environ.get('PEDIDO_TABLE')
    GMAIL_USER = os.environ.get('GMAIL_USER')
    GMAIL_PASSWORD = os.environ.get('GMAIL_PASSWORD')
    
    if not PEDIDO_TABLE:
        print("[Error] Falta variable de entorno PEDIDO_TABLE")
        return {'statusCode': 500, 'body': 'Error de configuración'}
    
    dynamodb = boto3.resource('dynamodb')
    
    try:
        # Procesar mensaje de SNS
        for record in event['Records']:
            sns_message = json.loads(record['Sns']['Message'])
            
            tenant_id = sns_message.get('tenant_id')
            uuid_pedido = sns_message.get('uuid')
            cliente_email = sns_message.get('cliente_email')
            cliente_nombre = sns_message.get('cliente_nombre', 'Cliente')
            
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
            pedido_clean = json.loads(json.dumps(pedido, default=decimal_default))
            
            # Calcular total del pedido
            total = 0.0
            for item in pedido_clean.get('elementos', []):
                precio = float(item.get('precio', 0))
                cantidad = int(item.get('cantidad_combo', 1))
                total += precio * cantidad
            
            # Enviar correo de confirmación
            enviar_email_confirmacion(
                cliente_email=cliente_email,
                cliente_nombre=cliente_nombre,
                uuid_pedido=uuid_pedido,
                total_pedido=total,
                smtp_user=GMAIL_USER,
                smtp_password=GMAIL_PASSWORD
            )
            
            print(f"[Success] Email enviado a: {cliente_email}")
    
    except Exception as e:
        print(f"[Error] Error enviando email: {str(e)}")
        return {'statusCode': 500, 'body': f'Error: {str(e)}'}
    
    return {'statusCode': 200, 'body': 'Email enviado exitosamente'}

def enviar_email_confirmacion(cliente_email, cliente_nombre, uuid_pedido, total_pedido, smtp_user, smtp_password):
    """Envía email de confirmación de pedido pagado"""
    
    # Asunto del correo
    subject = f"¡Tu pedido #{uuid_pedido} ha sido confirmado!"
    
    # HTML del correo adaptado para pedidos
    html_body = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Bembos – Pedido Confirmado</title>
    </head>

    <body style="margin:0; padding:0; background:#ececec; font-family:Arial, sans-serif;">
    <table width="100%" cellpadding="0" cellspacing="0" style="padding:40px 0;">
        <tr>
            <td align="center">
                <table width="640" cellpadding="0" cellspacing="0"
                    style="background:#ffffff; border-radius:14px; overflow:hidden;
                    box-shadow:0 5px 18px rgba(0,0,0,0.15);">
                    
                    <!-- HEADER PATRONES -->
                    <tr>
                        <td style="padding:0;">
                            <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
                                <tr>
                                    <td width="25%" style="background:#e60012;">
                                        <div style="height:120px;
                                            background-image: repeating-linear-gradient(135deg,
                                                #0060a0 0, #0060a0 10px,
                                                transparent 10px, transparent 20px);"></div>
                                    </td>
                                    <td width="25%" style="background:#ffd400;">
                                        <div style="height:120px;
                                            background-image: radial-gradient(circle,
                                                #e60012 0%, #e60012 12%, transparent 13%);
                                            background-size:42px 42px;
                                            background-position:center;"></div>
                                    </td>
                                    <td width="25%" style="background:#0060a0;">
                                        <div style="height:120px;
                                            background-image: radial-gradient(circle,
                                                #ffd400 0%, #ffd400 20%, transparent 21%);
                                            background-size:22px 22px;"></div>
                                    </td>
                                    <td width="25%" style="background:#ffd400;">
                                        <div style="height:120px;
                                            background-image: repeating-linear-gradient(45deg,
                                                #e60012 0, #e60012 6px,
                                                transparent 6px, transparent 12px);"></div>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>

                    <!-- LOGO -->
                    <tr>
                        <td align="center" style="padding:35px 20px 5px;">
                            <img src="https://upload.wikimedia.org/wikipedia/commons/d/d0/Bembos_logo15.png"
                            alt="Bembos" width="170" style="display:block;">
                        </td>
                    </tr>

                    <!-- TITULO -->
                    <tr>
                        <td align="center" style="padding:0 25px;">
                            <h1 style="margin:8px 0 0; font-size:33px; color:#002f6c; font-weight:900; letter-spacing:1px; text-transform:uppercase;">
                                ¡Pedido Confirmado!
                            </h1>
                        </td>
                    </tr>

                    <!-- TEXTO -->
                    <tr>
                        <td align="center" style="padding:22px 40px 28px; color:#444; font-size:18px; line-height:1.65;">
                            <p style="margin:0 0 14px; font-weight:500; letter-spacing:0.3px;">
                                Hola <strong style="color:#002f6c; font-weight:800;">{cliente_nombre}</strong>,
                            </p>
                            <p style="margin:0 0 14px; font-weight:500; letter-spacing:0.2px;">
                                ¡Tu pago ha sido procesado exitosamente! 
                            </p>
                            <p style="margin:0 0 20px; font-weight:500;">
                                <strong>Número de pedido:</strong> #{uuid_pedido}<br>
                                <strong>Total pagado:</strong> S/. {total_pedido:.2f}
                            </p>
                            <p style="margin:0; font-weight:500; background:#f0f9ff; padding:15px; border-radius:8px; border-left:4px solid #0060a0;">
                                Tu pedido está siendo preparado por nuestro equipo. 
                                Te notificaremos cuando esté listo para recoger.
                            </p>
                        </td>
                    </tr>

                    <!-- BOTÓN -->
                    <tr>
                        <td align="center" style="padding-bottom:38px;">
                            <a href="https://www.bembos.com.pe"
                            style="display:inline-block; background:#0060a0; padding:14px 48px; color:#ffffff;
                                font-size:18px; font-weight:900; text-decoration:none; border:2px solid #003f73;
                                letter-spacing:1px; transition:0.25s ease; text-transform:uppercase;"
                            onmouseover="this.style.background='transparent'; this.style.color='#003f73';"
                            onmouseout="this.style.background='#0060a0'; this.style.color='#ffffff';">
                                Ver mis pedidos
                            </a>
                        </td>
                    </tr>

                    <!-- FOOTER -->
                    <tr>
                        <td style="background:#f5f5f5; padding:18px 15px; color:#666; font-size:14px; text-align:center;">
                            © 2025 Bembos — ¡Gracias por tu preferencia!<br>
                            Para consultas: soporte@bembos.com
                        </td>
                    </tr>

                </table>
            </td>
        </tr>
    </table>
    </body>
    </html>
    """
    
    # Configuración del mensaje
    msg = MIMEMultipart('alternative')
    msg['From'] = smtp_user
    msg['To'] = cliente_email
    msg['Subject'] = subject
    
    # Adjuntar HTML
    msg.attach(MIMEText(html_body, 'html'))
    
    # Configuración SMTP
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587
    
    # Enviar el correo
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, cliente_email, msg.as_string())