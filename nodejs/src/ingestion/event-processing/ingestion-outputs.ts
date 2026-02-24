import { KafkaProducerWrapper } from '../../kafka/producer'

export const EVENTS_OUTPUT = 'events' as const
export const AI_EVENTS_OUTPUT = 'ai_events' as const

export type EventOutput = typeof EVENTS_OUTPUT
export type AiEventOutput = typeof EVENTS_OUTPUT | typeof AI_EVENTS_OUTPUT

export interface IngestionOutputConfig {
    topic: string
    producer: KafkaProducerWrapper
}

export class IngestionOutputs<O extends string> {
    private outputs: Record<O, IngestionOutputConfig>

    constructor(outputs: Record<O, IngestionOutputConfig>) {
        this.outputs = outputs
    }

    resolve(output: O): IngestionOutputConfig {
        return this.outputs[output]
    }
}
