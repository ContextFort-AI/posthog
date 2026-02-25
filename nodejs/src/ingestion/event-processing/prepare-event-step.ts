import { PluginEvent } from '~/plugin-scaffold'

import { EventHeaders, ISOTimestamp, PreIngestionEvent, Team } from '../../types'
import { sanitizeEventName } from '../../utils/db/utils'
import { logger } from '../../utils/logger'
import { captureException } from '../../utils/posthog'
import { invalidTimestampCounter } from '../../worker/ingestion/event-pipeline/metrics'
import { parseEventTimestamp } from '../../worker/ingestion/timestamps'
import { AI_EVENT_TYPES, processAiEvent } from '../ai'
import { PipelineWarning } from '../pipelines/pipeline.interface'
import { ok } from '../pipelines/results'
import { ProcessingStep } from '../pipelines/steps'

export type PrepareEventStepInput = {
    normalizedEvent: PluginEvent
    team: Team
    processPerson: boolean
    headers: EventHeaders
}

export type PrepareEventStepResult<TInput> = Omit<TInput, 'normalizedEvent'> & {
    preparedEvent: PreIngestionEvent
    historicalMigration: boolean
}

export function createPrepareEventStep<TInput extends PrepareEventStepInput>(): ProcessingStep<
    TInput,
    PrepareEventStepResult<TInput>
> {
    return function prepareEventStep(input: TInput) {
        const { normalizedEvent, ...rest } = input
        let event = normalizedEvent

        const warnings: PipelineWarning[] = []
        const invalidTimestampCallback = function (type: string, details: Record<string, any>) {
            invalidTimestampCounter.labels(type).inc()
            warnings.push({ type, details })
        }

        if (AI_EVENT_TYPES.has(event.event)) {
            try {
                event = processAiEvent(event)
            } catch (error) {
                captureException(error)
                logger.error(error)
            }
        }

        const properties = event.properties!
        const sanitizedEventName = sanitizeEventName(event['event'])

        if (properties['$ip'] && input.team.anonymize_ips) {
            delete properties['$ip']
        }

        const timestamp = parseEventTimestamp(event, invalidTimestampCallback)

        const preparedEvent: PreIngestionEvent = {
            eventUuid: event.uuid,
            event: sanitizedEventName,
            distinctId: String(event.distinct_id),
            properties,
            timestamp: timestamp.toISO() as ISOTimestamp,
            teamId: input.team.id,
            projectId: input.team.project_id,
        }

        const historicalMigration = input.headers.historical_migration ?? false

        return Promise.resolve(
            ok(
                {
                    ...rest,
                    preparedEvent,
                    historicalMigration,
                },
                [],
                warnings
            )
        )
    }
}
