import { createHmac, timingSafeEqual } from 'node:crypto';
import { SSMClient, GetParameterCommand } from '@aws-sdk/client-ssm';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import type { APIGatewayProxyEventV2, APIGatewayProxyResultV2 } from 'aws-lambda';
import { matchesRunnerRequest, parseWorkflowJobEvent } from './filter';
import type { WorkflowJobEvent } from './filter';
import type { RunnerRequestMessage } from './queue';

const WEBHOOK_SECRET_PARAM = process.env.WEBHOOK_SECRET_PARAM;
const REQUIRED_LABEL = process.env.REQUIRED_RUNNER_LABEL ?? 'lambda-microvms';
const ORG_OVERRIDE = process.env.GITHUB_ORG; // optional; otherwise derived from payload
const QUEUE_URL = process.env.QUEUE_URL;

const ssm = new SSMClient({});
const sqs = new SQSClient({});

function loadParam(name: string | undefined, envLabel: string): Promise<string> {
  if (!name) return Promise.reject(new Error(`${envLabel} env var is not set`));
  return ssm.send(new GetParameterCommand({ Name: name, WithDecryption: true })).then((res) => {
    const value = res.Parameter?.Value;
    if (!value) throw new Error(`SSM parameter ${name} has no value`);
    return value;
  });
}

let cachedSecret: Promise<string> | undefined;
const getWebhookSecret = (): Promise<string> =>
  (cachedSecret ??= loadParam(WEBHOOK_SECRET_PARAM, 'WEBHOOK_SECRET_PARAM').catch((err) => {
    cachedSecret = undefined;
    throw err;
  }));

// Warm up during init; no-op catch avoids an init-time unhandledRejection (handler re-awaits).
getWebhookSecret().catch(() => undefined);

const reply = (statusCode: number, body: string): APIGatewayProxyResultV2 => ({ statusCode, body });

export const handler = async (
  event: APIGatewayProxyEventV2
): Promise<APIGatewayProxyResultV2> => {
  // 1. Verify HMAC signature.
  let secret: string;
  try {
    secret = await getWebhookSecret();
  } catch (err) {
    console.error('Failed to load webhook secret:', err);
    return reply(500, 'secret unavailable');
  }

  const rawBody = event.body
    ? Buffer.from(event.body, event.isBase64Encoded ? 'base64' : 'utf8')
    : Buffer.alloc(0);
  const expected = 'sha256=' + createHmac('sha256', secret).update(rawBody).digest('hex');
  const received = Buffer.from(event.headers?.['x-hub-signature-256'] ?? '');
  const expectedBuf = Buffer.from(expected);
  if (!(received.length === expectedBuf.length && timingSafeEqual(received, expectedBuf))) {
    console.warn('Rejected webhook: invalid or missing signature');
    return reply(401, 'invalid signature');
  }

  // 2. Only act on workflow_job events (ping and others are acknowledged + ignored).
  const eventType = event.headers?.['x-github-event'];
  if (eventType !== 'workflow_job') {
    console.log(`Ignoring event type '${eventType ?? '(none)'}' (not workflow_job)`);
    return reply(200, 'ignored');
  }

  // 3. Parse + filter.
  let parsed: WorkflowJobEvent;
  try {
    parsed = parseWorkflowJobEvent(rawBody.toString('utf8'));
  } catch (err) {
    console.error('Failed to parse webhook body as JSON:', err);
    return reply(400, 'invalid json');
  }
  const match = matchesRunnerRequest(parsed, REQUIRED_LABEL);
  if (!match.matched) {
    console.log(`Ignoring workflow_job event: ${match.reason}`);
    return reply(200, 'ignored');
  }
  console.log(`Matched runner request: ${match.reason}`);

  // 4. Resolve target org.
  const org = ORG_OVERRIDE ?? parsed.organization?.login;
  if (!org) {
    console.error('Cannot determine organization from payload (organization.login missing)');
    return reply(422, 'missing organization');
  }

  // 5. Enqueue the runner request for async processing by the worker Lambda.
  const runId = parsed.workflow_job?.run_id;
  const labels = parsed.workflow_job?.labels ?? [REQUIRED_LABEL];

  if (!QUEUE_URL) {
    console.error('QUEUE_URL env var is not set');
    return reply(500, 'queue not configured');
  }

  const message: RunnerRequestMessage = { org, runId, labels };
  try {
    const result = await sqs.send(
      new SendMessageCommand({
        QueueUrl: QUEUE_URL,
        MessageBody: JSON.stringify(message),
      })
    );
    console.log(`[${runId}] Enqueued runner request for org '${org}', labels: ${labels.join(', ')} — MessageId: ${result.MessageId}`);
    return reply(202, 'queued');
  } catch (err) {
    console.error(`[${runId}] Failed to enqueue runner request:`, err);
    return reply(500, 'enqueue failed');
  }
};
