import { SSMClient, GetParameterCommand } from '@aws-sdk/client-ssm';
import type { SQSHandler } from 'aws-lambda';
import {
  buildRunnerName,
  generateOrgJitConfig,
  getInstallationToken,
  mintAppJwt,
  parseAppCredentials,
} from './github';
import { buildMicrovmConfig, selectImageIdentifier, runMicrovmWithRetry } from './microvms';
import type { MicrovmLaunchConfig } from './microvms';
import type { RunnerRequestMessage } from './queue';

const APP_CREDENTIALS_PARAM = process.env.GITHUB_APP_CREDENTIALS_PARAM;
const RUNNER_GROUP_ID = Number(process.env.RUNNER_GROUP_ID ?? '1');

const ssm = new SSMClient({});

function loadParam(name: string | undefined, envLabel: string): Promise<string> {
  if (!name) return Promise.reject(new Error(`${envLabel} env var is not set`));
  return ssm.send(new GetParameterCommand({ Name: name, WithDecryption: true })).then((res) => {
    const value = res.Parameter?.Value;
    if (!value) throw new Error(`SSM parameter ${name} has no value`);
    return value;
  });
}

let cachedCreds: Promise<string> | undefined;
const getAppCredentials = (): Promise<string> =>
  (cachedCreds ??= loadParam(APP_CREDENTIALS_PARAM, 'GITHUB_APP_CREDENTIALS_PARAM').catch((err) => {
    cachedCreds = undefined;
    throw err;
  }));

// Warm up during init; no-op catch avoids an init-time unhandledRejection (handler re-awaits).
getAppCredentials().catch(() => undefined);

const microvmConfig = buildMicrovmConfig();

export const handler: SQSHandler = async (event) => {
  const record = event.Records[0];
  const messageId = record.messageId;

  // Parse the message body.
  let message: RunnerRequestMessage;
  try {
    message = JSON.parse(record.body) as RunnerRequestMessage;
  } catch (err) {
    // Malformed JSON — throw so the message is not deleted and eventually redrive to DLQ.
    console.error(`[${messageId}] Failed to parse SQS message body:`, err);
    throw err;
  }

  const { org, runId, labels } = message;
  console.log(`[${runId}] Worker received message ${messageId} for org '${org}', labels: ${labels.join(', ')}`);

  // Validate microvm config — if misconfigured, throw to cause requeue.
  if (typeof microvmConfig === 'string') {
    const configErr = new Error(`MicroVM misconfigured: ${microvmConfig}`);
    console.error(`[${runId}] ${configErr.message}`);
    throw configErr;
  }

  // Mint GitHub JIT config, then launch the MicroVM. On any failure, log the stage that
  // was in flight and rethrow so SQS does not delete the message (it becomes visible again
  // after the visibility timeout and eventually redrives to the DLQ after maxReceiveCount).
  let stage = 'getAppCredentials';
  const startedAt = Date.now();
  let t = startedAt;

  try {
    const credsRaw = await getAppCredentials();
    console.log(`[${runId}] getAppCredentials: ${Date.now() - t}ms`);

    stage = 'parseAppCredentials+mintJwt';
    const creds = parseAppCredentials(credsRaw);
    const jwt = mintAppJwt(creds.appClientId, creds.privateKey);

    stage = 'getInstallationToken';
    t = Date.now();
    const token = await getInstallationToken(jwt, creds.installationId);
    console.log(`[${runId}] getInstallationToken: ${Date.now() - t}ms`);

    stage = 'generateOrgJitConfig';
    t = Date.now();
    const runnerName = buildRunnerName(runId);
    const jit = await generateOrgJitConfig(token, org, {
      name: runnerName,
      runnerGroupId: RUNNER_GROUP_ID,
      labels,
    });
    console.log(`[${runId}] generateOrgJitConfig: ${Date.now() - t}ms — JIT runner created:`, JSON.stringify(jit.runner));

    stage = 'runMicrovmWithRetry';
    t = Date.now();
    const imageIdentifier = selectImageIdentifier(labels, microvmConfig.images, microvmConfig.dockerLabel);
    const hasDockerLabel = labels.includes(microvmConfig.dockerLabel);
    console.log(`[${runId}] Launching ${hasDockerLabel ? 'docker' : 'no-docker'} image: ${imageIdentifier}`);
    const launchConfig: MicrovmLaunchConfig = { ...microvmConfig.base, imageIdentifier };
    const vm = await runMicrovmWithRetry(launchConfig, jit.encoded_jit_config, {
      attempts: 3,
      delayMs: 5000,
    });
    console.log(`[${runId}] runMicrovmWithRetry: ${Date.now() - t}ms total — MicroVM launched: ${vm.microvmId}, endpoint: ${vm.endpoint}`);
    console.log(`[${runId}] Worker completed message ${messageId} in ${Date.now() - startedAt}ms`);
  } catch (err) {
    console.error(`[${runId}] Worker failed at stage '${stage}' after ${Date.now() - startedAt}ms`, err);
    throw err;
  }
};
