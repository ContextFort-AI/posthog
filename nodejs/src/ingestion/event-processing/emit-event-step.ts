import { DateTime } from 'luxon'
import { Message } from 'node-rdkafka'

import { ingestionLagGauge, ingestionLagHistogram } from '../../common/metrics'
import { EventHeaders, PersonMode, ProjectId, TeamId, TimestampFormat } from '../../types'
import { MessageSizeTooLarge } from '../../utils/db/error'
import { safeClickhouseString } from '../../utils/db/utils'
import { castTimestampOrNow, castTimestampToClickhouseFormat } from '../../utils/utils'
import { eventProcessedAndIngestedCounter } from '../../worker/ingestion/event-pipeline/metrics'
import { captureIngestionWarning } from '../../worker/ingestion/utils'
import { ok } from '../pipelines/results'
import { ProcessingStep } from '../pipelines/steps'
import { IngestionOutputs } from './ingestion-outputs'

// Event after pipeline processing, before ClickHouse/Kafka serialization.
// All fields use native types — serialization happens in the emit step.
export interface ProcessedEvent {
    uuid: string
    event: string
    team_id: TeamId
    project_id: ProjectId
    distinct_id: string
    properties: Record<string, unknown>
    timestamp: DateTime | string
    elements_chain: string
    created_at: DateTime | null
    captured_at: Date | null
    person_id: string
    person_properties: Record<string, unknown>
    person_created_at: DateTime | null
    person_mode: PersonMode
    historical_migration?: boolean
}

export interface EventToEmit<O extends string = string> {
    event: ProcessedEvent
    output: O
}

export interface EmitEventStepConfig<O extends string = string> {
    outputs: IngestionOutputs<O>
    groupId: string
}

export interface EmitEventStepInput<O extends string = string> {
    eventsToEmit: EventToEmit<O>[]
    headers: EventHeaders
    message: Message
}

export function createEmitEventStep<O extends string, T extends EmitEventStepInput<O>>(
    config: EmitEventStepConfig<O>
): ProcessingStep<T, void> {
    return function emitEventStep(input) {
        const { eventsToEmit, headers, message } = input
        const { outputs, groupId } = config

        // Record ingestion lag metric if we have the required data
        if (headers?.now && message?.topic !== undefined && message?.partition !== undefined) {
            const lag = Date.now() - headers.now.getTime()
            ingestionLagGauge.labels({ topic: message.topic, partition: String(message.partition), groupId }).set(lag)
            ingestionLagHistogram.labels({ groupId, partition: String(message.partition) }).observe(lag)
        }

        // TODO: It's not great that we put the produce outcome in side effects, we should probably await it here
        //       but it might slow the pipeline down. Historically, it has always been like that.
        //       We should investigate this later.
        const sideEffects = eventsToEmit.map(({ event, output }) => {
            const { topic, producer } = outputs.resolve(output)
            return producer
                .produce({
                    topic,
                    key: event.uuid,
                    value: Buffer.from(serializeEvent(event)),
                    headers: { productTrack: productTrackHeader(event) },
                })
                .then((result) => {
                    eventProcessedAndIngestedCounter.inc()
                    return result
                })
                .catch(async (error) => {
                    // TODO: For now we have to live with the ingestion warning happening here
                    //       Once the batch pipelines support warnings, we'll put it in the result
                    // Some messages end up significantly larger than the original
                    // after plugin processing, person & group enrichment, etc.
                    if (error instanceof MessageSizeTooLarge) {
                        await captureIngestionWarning(producer, event.team_id, 'message_size_too_large', {
                            eventUuid: event.uuid,
                            distinctId: event.distinct_id,
                        })
                    } else {
                        throw error
                    }
                })
        })

        return Promise.resolve(ok(undefined, sideEffects))
    }
}

function serializeEvent(event: ProcessedEvent): string {
    const timestamp =
        typeof event.timestamp === 'string'
            ? castTimestampOrNow(event.timestamp, TimestampFormat.ClickHouse)
            : castTimestampToClickhouseFormat(event.timestamp, TimestampFormat.ClickHouse)

    return JSON.stringify({
        uuid: event.uuid,
        event: safeClickhouseString(event.event),
        properties: JSON.stringify(event.properties),
        timestamp,
        team_id: event.team_id,
        project_id: event.project_id,
        distinct_id: safeClickhouseString(event.distinct_id),
        elements_chain: safeClickhouseString(event.elements_chain),
        created_at: castTimestampOrNow(event.created_at, TimestampFormat.ClickHouse),
        captured_at:
            event.captured_at !== null
                ? castTimestampToClickhouseFormat(DateTime.fromJSDate(event.captured_at), TimestampFormat.ClickHouse)
                : null,
        person_id: event.person_id,
        person_properties: JSON.stringify(event.person_properties),
        person_created_at: castTimestampOrNow(event.person_created_at, TimestampFormat.ClickHouseSecondPrecision),
        person_mode: event.person_mode,
        ...(event.historical_migration ? { historical_migration: true } : {}),
    })
}

export function productTrackHeader(event: ProcessedEvent): string {
    return event.event.startsWith('$ai_') ? 'llma' : 'general'
}
