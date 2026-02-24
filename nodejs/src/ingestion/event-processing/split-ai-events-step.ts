import { RawKafkaEvent } from '../../types'
import { parseJSON } from '../../utils/json-parse'
import { ok } from '../pipelines/results'
import { ProcessingStep } from '../pipelines/steps'
import { EventToEmit } from './emit-event-step'

const LARGE_AI_PROPERTIES = new Set([
    '$ai_input',
    '$ai_output',
    '$ai_output_choices',
    '$ai_input_state',
    '$ai_output_state',
    '$ai_tools',
])

export interface SplitAiEventsStepConfig {
    aiEventsTopic: string
}

export interface SplitAiEventsStepInput {
    eventsToEmit: EventToEmit[]
}

function maybeStripAiProperties(entry: EventToEmit, aiEventsTopic: string): EventToEmit[] {
    if (entry.topic === aiEventsTopic) {
        return [entry]
    }

    // TODO: review whether we can serialize in emit-event-step instead, to avoid
    //       parsing and re-serializing properties here
    const properties: Record<string, unknown> = entry.event.properties ? parseJSON(entry.event.properties) : {}

    let hasLarge = false
    for (const key of LARGE_AI_PROPERTIES) {
        if (key in properties) {
            hasLarge = true
            break
        }
    }

    if (!hasLarge) {
        return [entry]
    }

    const stripped: Record<string, unknown> = {}
    for (const [key, value] of Object.entries(properties)) {
        if (!LARGE_AI_PROPERTIES.has(key)) {
            stripped[key] = value
        }
    }

    const strippedEvent: RawKafkaEvent = { ...entry.event, properties: JSON.stringify(stripped) }

    return [
        { event: strippedEvent, topic: entry.topic },
        { event: entry.event, topic: aiEventsTopic },
    ]
}

export function createSplitAiEventsStep<T extends SplitAiEventsStepInput>(
    config: SplitAiEventsStepConfig
): ProcessingStep<T, T> {
    const { aiEventsTopic } = config

    return function splitAiEventsStep(input) {
        return Promise.resolve(
            ok({
                ...input,
                eventsToEmit: input.eventsToEmit.flatMap((entry) => maybeStripAiProperties(entry, aiEventsTopic)),
            })
        )
    }
}
