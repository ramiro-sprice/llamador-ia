# Llamador IA Argentina

MVP web que inicia una llamada saliente, reproduce un mensaje fijo y luego mantiene una conversación con IA en español. Usa Twilio ConversationRelay para telefonía, transcripción y voz; OpenAI genera las respuestas.

## Probar sin llamar

```bash
npm install
copy .env.example .env
npm start
```

Abrí `http://localhost:3000`. El modo simulación viene activado y no consume servicios externos.

## Activar llamadas reales

1. Creá cuentas de Twilio y OpenAI y completá `.env`.
2. En Twilio, habilitá ConversationRelay y aceptá su anexo de IA.
3. Exponé el servidor con HTTPS/WSS (por ejemplo, mediante un túnel durante desarrollo) y colocá esa URL en `PUBLIC_URL`.
4. Verificá en Twilio los permisos de llamadas salientes a Argentina y el identificador de origen permitido.
5. Configurá `CALL_ADMIN_TOKEN` con una clave aleatoria de al menos 16 caracteres. Sin ella, las llamadas reales quedan bloqueadas.
6. Reiniciá el servidor, desmarcá “Modo simulación” y probá primero con tu propio teléfono.

Los números se aceptan en formato E.164 argentino (`+54...`). Los celulares suelen escribirse para llamadas internacionales como `+549` seguido del código de área y número, sin 0 ni 15.

## Seguridad y límites

- El mensaje debe informar que se trata de una llamada automatizada con IA.
- Solo se debe llamar a destinatarios con consentimiento; no usar para campañas masivas, suplantación, cobranzas abusivas ni emergencias.
- La duración predeterminada es cinco minutos y tiene un límite técnico de quince.
- Este MVP guarda estado y transcripciones solo en memoria: se pierden al reiniciar.
- Antes de producción, agregá autenticación al panel, persistencia cifrada, control de acceso, rate limiting, validación de firmas Twilio y una revisión legal argentina específica para tu caso de uso.
