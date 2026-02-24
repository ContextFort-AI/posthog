import { Message } from 'node-rdkafka'

import { ingestionLagGauge, ingestionLagHistogram } from '../../common/metrics'
import { KafkaProducerWrapper } from '../../kafka/producer'
import { EventHeaders, RawKafkaEvent } from '../../types'
import { MessageSizeTooLarge } from '../../utils/db/error'
import { eventProcessedAndIngestedCounter } from '../../worker/ingestion/event-pipeline/metrics'
import { captureIngestionWarning } from '../../worker/ingestion/utils'
import { ok } from '../pipelines/results'
import { ProcessingStep } from '../pipelines/steps'

// TODO: Create a Kafka destination registry. EventToEmit should use a type-safe destination enum
//       (e.g. 'events' | 'ai_events') instead of raw topic strings. The ingestion consumer sets up
//       the registry mapping destination names to topic names and producer instances, decoupling
//       Kafka infrastructure from event destinations.
export interface EventToEmit {
    event: RawKafkaEvent
    topic: string
}

export interface EmitEventStepConfig {
    kafkaProducer: KafkaProducerWrapper
    groupId: string
}

export interface EmitEventStepInput {
    eventsToEmit: EventToEmit[]
    headers: EventHeaders
    message: Message
}

export function createEmitEventStep<T extends EmitEventStepInput>(
    config: EmitEventStepConfig
): ProcessingStep<T, void> {
    return function emitEventStep(input) {
        const { eventsToEmit, headers, message } = input
        const { kafkaProducer, groupId } = config

        // Record ingestion lag metric if we have the required data
        if (headers?.now && message?.topic !== undefined && message?.partition !== undefined) {
            const lag = Date.now() - headers.now.getTime()
            ingestionLagGauge.labels({ topic: message.topic, partition: String(message.partition), groupId }).set(lag)
            ingestionLagHistogram.labels({ groupId, partition: String(message.partition) }).observe(lag)
        }

        // TODO: It's not great that we put the produce outcome in side effects, we should probably await it here
        //       but it might slow the pipeline down. Historically, it has always been like that.
        //       We should investigate this later.
        const sideEffects = eventsToEmit.map(({ event, topic }) =>
            kafkaProducer
                .produce({
                    topic,
                    key: event.uuid,
                    // TODO: Build a ClickHouse Kafka serializer here and remove JSON encoding
                    //       from upstream steps (create-event, split-ai-events) to avoid
                    //       redundant parse/serialize round-trips.
                    value: Buffer.from(JSON.stringify(event)),
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
                        await captureIngestionWarning(kafkaProducer, event.team_id, 'message_size_too_large', {
                            eventUuid: event.uuid,
                            distinctId: event.distinct_id,
                        })
                    } else {
                        throw error
                    }
                })
        )

        return Promise.resolve(ok(undefined, sideEffects))
    }
}

export function productTrackHeader(event: RawKafkaEvent): string {
    return event.event.startsWith('$ai_') ? 'llma' : 'general'
}
