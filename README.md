# Notification Microservice

Microsserviço em Go para receber, agendar e processar notificações destinadas a usuários. Ele separa a solicitação de uma notificação do seu processamento: a API aceita o pedido rapidamente, enquanto o trabalho segue de forma assíncrona. Isso permite enviar notificações imediatamente ou programá-las para um horário futuro, sem manter o cliente esperando pelo processamento.

Cada notificação contém um usuário, uma mensagem, um canal (`type`: web, push, SMS ou e-mail) e, opcionalmente, um horário de agendamento (`scheduled_at`). O serviço registra as notificações para acompanhar seu ciclo de vida. O scheduler busca itens pendentes cujo horário chegou, tenta entregá-los pelo provider correspondente e atualiza o status. Falhas podem ser tentadas novamente; notificações que esgotam as tentativas ficam marcadas como `failed`.

Os providers de web, push, SMS e e-mail são atualmente implementações demonstrativas: registram a tentativa nos logs, mas não integram com gateways externos de entrega.

O producer expõe a API HTTP e publica mensagens no RabbitMQ. O consumer grava as mensagens no MongoDB e um scheduler processa as notificações pendentes.

## Fluxo

`Cliente → Producer API → RabbitMQ → Consumer → MongoDB → Scheduler`

O projeto inclui quatro serviços no Docker Compose: `producer`, `consumer`, `rabbitmq` e `mongodb`.

## Executar

Na raiz do repositório:

```bash
docker compose up --build -d
docker compose ps
```

Para acompanhar os logs ou encerrar:

```bash
docker compose logs -f
docker compose down
```

Os dados do MongoDB e do RabbitMQ persistem em volumes nomeados. `docker compose down -v` também remove esses dados.

## API

Endpoint: `POST http://localhost:8080/send-notification`

Campos obrigatórios: `user_id`, `message` e `type`. Valores de `type`: `web`, `push`, `sms` ou `email`. `scheduled_at` é opcional, deve estar em RFC 3339 com fuso horário explícito e precisa ser posterior ao minuto atual do servidor.

Exemplo de envio imediato:

```bash
curl -X POST http://localhost:8080/send-notification \
  -H "Content-Type: application/json" \
  -d '{"user_id":"user-123","message":"Olá!","type":"web"}'
```

Exemplo agendado:

```json
{
  "user_id": "user-123",
  "message": "Olá!",
  "type": "web",
  "scheduled_at": "2026-10-01T14:30:00Z"
}
```

As respostas HTTP usam JSON com `message`, `status` (código HTTP) e `data` quando houver conteúdo.

## Dependências locais

- RabbitMQ AMQP: `localhost:5672`
- RabbitMQ Management: `http://localhost:15672` (padrão local: `guest`/`guest`)
- MongoDB: `mongodb://admin:secret@localhost:27017/?authSource=admin`

Copie `.env.example` para `.env` para ajustar portas e credenciais. Após alterar o código, reconstrua as imagens com `docker compose up --build -d`.
