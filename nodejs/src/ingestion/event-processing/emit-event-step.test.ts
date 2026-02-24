import { Message } from 'node-rdkafka'

import { createTestEventHeaders } from '../../../tests/helpers/event-headers'
import { createTestMessage } from '../../../tests/helpers/kafka-message'
import { ingestionLagGauge, ingestionLagHistogram } from '../../common/metrics'
import { KafkaProducerWrapper } from '../../kafka/producer'
import { EventHeaders, ProjectId, RawKafkaEvent, TimestampFormat } from '../../types'
import { MessageSizeTooLarge } from '../../utils/db/error'
import { castTimestampOrNow } from '../../utils/utils'
import { eventProcessedAndIngestedCounter } from '../../worker/ingestion/event-pipeline/metrics'
import { captureIngestionWarning } from '../../worker/ingestion/utils'
import { isOkResult } from '../pipelines/results'
import { EmitEventStepInput, createEmitEventStep, productTrackHeader } from './emit-event-step'
import { EVENTS_OUTPUT, IngestionOutputs } from './ingestion-outputs'

jest.mock('../../worker/ingestion/utils', () => ({
    captureIngestionWarning: jest.fn().mockResolvedValue(undefined),
}))

jest.mock('../../worker/ingestion/event-pipeline/metrics', () => ({
    eventProcessedAndIngestedCounter: {
        inc: jest.fn(),
    },
}))

jest.mock('~/common/metrics', () => ({
    ingestionLagGauge: {
        labels: jest.fn().mockReturnValue({
            set: jest.fn(),
        }),
    },
    ingestionLagHistogram: {
        labels: jest.fn().mockReturnValue({
            observe: jest.fn(),
        }),
    },
}))

const mockCaptureIngestionWarning = jest.mocked(captureIngestionWarning)
const mockEventProcessedAndIngestedCounter = jest.mocked(eventProcessedAndIngestedCounter)
const mockIngestionLagGauge = jest.mocked(ingestionLagGauge)
const mockIngestionLagHistogram = jest.mocked(ingestionLagHistogram)

describe('emit-event-step', () => {
    let mockKafkaProducer: jest.Mocked<KafkaProducerWrapper>
    let mockRawEvent: RawKafkaEvent
    let mockHeaders: EventHeaders
    let mockMessage: Message

    beforeEach(() => {
        mockHeaders = createTestEventHeaders()
        mockMessage = createTestMessage()
        jest.clearAllMocks()

        mockKafkaProducer = {
            produce: jest.fn().mockResolvedValue(undefined),
            flush: jest.fn().mockResolvedValue(undefined),
            disconnect: jest.fn().mockResolvedValue(undefined),
        } as any

        const testTimestamp = castTimestampOrNow('2023-01-01T00:00:00.000Z', TimestampFormat.ClickHouse)

        mockRawEvent = {
            uuid: 'test-uuid',
            event: 'test-event',
            properties: JSON.stringify({ test: 'property' }),
            timestamp: testTimestamp,
            team_id: 1,
            project_id: 1 as ProjectId,
            distinct_id: 'test-distinct-id',
            elements_chain: '',
            created_at: testTimestamp,
            person_id: 'person-uuid',
            person_properties: JSON.stringify({}),
            person_created_at: testTimestamp,
            person_mode: 'full',
            historical_migration: false,
        }
    })

    function createOutputs(
        config: Record<string, { topic: string }> = {
            [EVENTS_OUTPUT]: { topic: 'clickhouse_events_json' },
        }
    ): IngestionOutputs<string> {
        const mapped = Object.fromEntries(
            Object.entries(config).map(([dest, { topic }]) => [dest, { topic, producer: mockKafkaProducer }])
        ) as Record<string, { topic: string; producer: typeof mockKafkaProducer }>
        return new IngestionOutputs(mapped)
    }

    const createInput = (overrides: Partial<EmitEventStepInput> = {}): EmitEventStepInput => ({
        eventsToEmit: [{ event: mockRawEvent, output: EVENTS_OUTPUT }],
        headers: mockHeaders,
        message: mockMessage,
        ...overrides,
    })

    describe('createEmitEventStep', () => {
        it('should emit a single event to its output topic', async () => {
            const step = createEmitEventStep({
                outputs: createOutputs(),
                groupId: 'test-group-id',
            })
            const result = await step(createInput())

            expect(isOkResult(result)).toBe(true)
            expect(result.sideEffects).toHaveLength(1)
            expect(mockKafkaProducer.produce).toHaveBeenCalledWith({
                topic: 'clickhouse_events_json',
                key: 'test-uuid',
                value: Buffer.from(JSON.stringify(mockRawEvent)),
                headers: { productTrack: 'general' },
            })

            await result.sideEffects[0]
            expect(mockEventProcessedAndIngestedCounter.inc).toHaveBeenCalledTimes(1)
        })

        it('should emit multiple events to their respective output topics', async () => {
            const secondEvent = { ...mockRawEvent, uuid: 'second-uuid', event: '$ai_generation' }
            const outputs = createOutputs({
                events: { topic: 'clickhouse_events_json' },
                ai_events: { topic: 'clickhouse_ai_events_json' },
            })
            const step = createEmitEventStep({
                outputs,
                groupId: 'test-group-id',
            })
            const result = await step(
                createInput({
                    eventsToEmit: [
                        { event: mockRawEvent, output: 'events' },
                        { event: secondEvent, output: 'ai_events' },
                    ],
                })
            )

            expect(result.sideEffects).toHaveLength(2)
            expect(mockKafkaProducer.produce).toHaveBeenCalledTimes(2)
            expect(mockKafkaProducer.produce).toHaveBeenCalledWith({
                topic: 'clickhouse_events_json',
                key: 'test-uuid',
                value: Buffer.from(JSON.stringify(mockRawEvent)),
                headers: { productTrack: 'general' },
            })
            expect(mockKafkaProducer.produce).toHaveBeenCalledWith({
                topic: 'clickhouse_ai_events_json',
                key: 'second-uuid',
                value: Buffer.from(JSON.stringify(secondEvent)),
                headers: { productTrack: 'llma' },
            })

            await Promise.all(result.sideEffects)
            expect(mockEventProcessedAndIngestedCounter.inc).toHaveBeenCalledTimes(2)
        })

        it('should handle empty eventsToEmit list', async () => {
            const step = createEmitEventStep({
                outputs: createOutputs(),
                groupId: 'test-group-id',
            })
            const result = await step(createInput({ eventsToEmit: [] }))

            expect(isOkResult(result)).toBe(true)
            expect(result.sideEffects).toHaveLength(0)
            expect(mockKafkaProducer.produce).not.toHaveBeenCalled()
        })

        it('should handle MessageSizeTooLarge error and capture ingestion warning', async () => {
            mockKafkaProducer.produce.mockRejectedValue(
                new MessageSizeTooLarge('Message too large', new Error('Kafka error'))
            )

            const step = createEmitEventStep({
                outputs: createOutputs(),
                groupId: 'test-group-id',
            })
            const result = await step(createInput())

            expect(isOkResult(result)).toBe(true)
            expect(result.sideEffects).toHaveLength(1)

            await result.sideEffects[0]

            expect(mockCaptureIngestionWarning).toHaveBeenCalledWith(mockKafkaProducer, 1, 'message_size_too_large', {
                eventUuid: 'test-uuid',
                distinctId: 'test-distinct-id',
            })
            expect(mockEventProcessedAndIngestedCounter.inc).not.toHaveBeenCalled()
        })

        it('should not increment metric when Kafka produce fails', async () => {
            mockKafkaProducer.produce.mockRejectedValue(new Error('Kafka connection failed'))

            const step = createEmitEventStep({
                outputs: createOutputs(),
                groupId: 'test-group-id',
            })
            const result = await step(createInput())

            expect(result.sideEffects).toHaveLength(1)
            await expect(result.sideEffects[0]).rejects.toThrow('Kafka connection failed')
            expect(mockEventProcessedAndIngestedCounter.inc).not.toHaveBeenCalled()
        })

        it('should increment metric once per event in list', async () => {
            const secondEvent = { ...mockRawEvent, uuid: 'second-uuid' }
            const outputs = createOutputs({
                dest_a: { topic: 'topic_a' },
                dest_b: { topic: 'topic_b' },
            })
            const step = createEmitEventStep({
                outputs,
                groupId: 'test-group-id',
            })
            const result = await step(
                createInput({
                    eventsToEmit: [
                        { event: mockRawEvent, output: 'dest_a' },
                        { event: secondEvent, output: 'dest_b' },
                    ],
                })
            )

            expect(result.sideEffects).toHaveLength(2)
            await Promise.all(result.sideEffects)
            expect(mockEventProcessedAndIngestedCounter.inc).toHaveBeenCalledTimes(2)
            expect(mockKafkaProducer.produce).toHaveBeenCalledTimes(2)
        })

        it('should re-throw non-MessageSizeTooLarge errors', async () => {
            mockKafkaProducer.produce.mockRejectedValue(new Error('Generic Kafka error'))

            const step = createEmitEventStep({
                outputs: createOutputs(),
                groupId: 'test-group-id',
            })
            const result = await step(createInput())

            expect(result.sideEffects).toHaveLength(1)
            await expect(result.sideEffects[0]).rejects.toThrow('Generic Kafka error')
            expect(mockCaptureIngestionWarning).not.toHaveBeenCalled()
            expect(mockEventProcessedAndIngestedCounter.inc).not.toHaveBeenCalled()
        })

        it('should emit AI events with llma product track header', async () => {
            const aiEvent = { ...mockRawEvent, event: '$ai_generation' }
            const step = createEmitEventStep({
                outputs: createOutputs(),
                groupId: 'test-group-id',
            })
            await step(createInput({ eventsToEmit: [{ event: aiEvent, output: EVENTS_OUTPUT }] }))

            expect(mockKafkaProducer.produce).toHaveBeenCalledWith({
                topic: 'clickhouse_events_json',
                key: aiEvent.uuid,
                value: Buffer.from(JSON.stringify(aiEvent)),
                headers: { productTrack: 'llma' },
            })
        })
    })

    describe('productTrackHeader', () => {
        it.each([
            { event: '$ai_generation', expected: 'llma' },
            { event: '$ai_completion', expected: 'llma' },
            { event: '$pageview', expected: 'general' },
            { event: 'user_signed_up', expected: 'general' },
        ])('should return "$expected" for $event', ({ event, expected }) => {
            expect(productTrackHeader({ ...mockRawEvent, event })).toBe(expected)
        })
    })

    describe('ingestion lag metric', () => {
        const FAKE_NOW_MS = 1702654321987
        let mockSetFn: jest.Mock
        let mockObserveFn: jest.Mock

        const createMessage = (overrides: Partial<Message> = {}): Message => ({
            value: Buffer.from('test-value'),
            key: Buffer.from('test-key'),
            offset: 100,
            partition: 5,
            topic: 'test-topic',
            size: 10,
            ...overrides,
        })

        const createHeaders = (overrides: Partial<EventHeaders> = {}): EventHeaders => ({
            force_disable_person_processing: false,
            historical_migration: false,
            ...overrides,
        })

        beforeEach(() => {
            jest.useFakeTimers()
            jest.setSystemTime(FAKE_NOW_MS)

            mockSetFn = jest.fn()
            mockObserveFn = jest.fn()
            mockIngestionLagGauge.labels.mockReturnValue({ set: mockSetFn } as any)
            mockIngestionLagHistogram.labels.mockReturnValue({ observe: mockObserveFn } as any)
        })

        afterEach(() => {
            jest.useRealTimers()
        })

        it('should record ingestion lag when headers.now and message are present', async () => {
            const captureTime = new Date(FAKE_NOW_MS - 5432)
            const step = createEmitEventStep({
                outputs: createOutputs(),
                groupId: 'test-group-id',
            })
            await step(createInput({ headers: createHeaders({ now: captureTime }), message: createMessage() }))

            expect(mockIngestionLagGauge.labels).toHaveBeenCalledWith({
                topic: 'test-topic',
                partition: '5',
                groupId: 'test-group-id',
            })
            expect(mockSetFn).toHaveBeenCalledWith(5432)
        })

        it('should not record ingestion lag when headers.now is missing', async () => {
            const step = createEmitEventStep({
                outputs: createOutputs(),
                groupId: 'test-group-id',
            })
            await step(createInput({ headers: createHeaders(), message: createMessage() }))

            expect(mockIngestionLagGauge.labels).not.toHaveBeenCalled()
        })

        it.each([
            { desc: 'message.topic is undefined', messageOverride: { topic: undefined as unknown as string } },
            { desc: 'message.partition is undefined', messageOverride: { partition: undefined as unknown as number } },
        ])('should not record ingestion lag when $desc', async ({ messageOverride }) => {
            const step = createEmitEventStep({
                outputs: createOutputs(),
                groupId: 'test-group-id',
            })
            await step(
                createInput({
                    headers: createHeaders({ now: new Date(FAKE_NOW_MS - 1000) }),
                    message: createMessage(messageOverride),
                })
            )

            expect(mockIngestionLagGauge.labels).not.toHaveBeenCalled()
        })

        it('should use groupId from config in metric labels', async () => {
            const step = createEmitEventStep({
                outputs: createOutputs(),
                groupId: 'custom-consumer-group',
            })
            await step(
                createInput({
                    headers: createHeaders({ now: new Date(FAKE_NOW_MS - 1000) }),
                    message: createMessage(),
                })
            )

            expect(mockIngestionLagGauge.labels).toHaveBeenCalledWith({
                topic: 'test-topic',
                partition: '5',
                groupId: 'custom-consumer-group',
            })
        })

        it('should handle partition 0 correctly', async () => {
            const step = createEmitEventStep({
                outputs: createOutputs(),
                groupId: 'test-group-id',
            })
            await step(
                createInput({
                    headers: createHeaders({ now: new Date(FAKE_NOW_MS - 1000) }),
                    message: createMessage({ partition: 0 }),
                })
            )

            expect(mockIngestionLagGauge.labels).toHaveBeenCalledWith({
                topic: 'test-topic',
                partition: '0',
                groupId: 'test-group-id',
            })
        })

        describe('histogram', () => {
            it('should observe lag in histogram with correct labels', async () => {
                const step = createEmitEventStep({
                    outputs: createOutputs(),
                    groupId: 'test-group-id',
                })
                await step(
                    createInput({
                        headers: createHeaders({ now: new Date(FAKE_NOW_MS - 5432) }),
                        message: createMessage(),
                    })
                )

                expect(mockIngestionLagHistogram.labels).toHaveBeenCalledWith({
                    groupId: 'test-group-id',
                    partition: '5',
                })
                expect(mockObserveFn).toHaveBeenCalledWith(5432)
            })

            it('should use custom groupId in histogram labels', async () => {
                const step = createEmitEventStep({
                    outputs: createOutputs(),
                    groupId: 'custom-consumer-group',
                })
                await step(
                    createInput({
                        headers: createHeaders({ now: new Date(FAKE_NOW_MS - 1000) }),
                        message: createMessage({ partition: 3 }),
                    })
                )

                expect(mockIngestionLagHistogram.labels).toHaveBeenCalledWith({
                    groupId: 'custom-consumer-group',
                    partition: '3',
                })
                expect(mockObserveFn).toHaveBeenCalledWith(1000)
            })

            it('should not observe histogram when headers.now is missing', async () => {
                const step = createEmitEventStep({
                    outputs: createOutputs(),
                    groupId: 'test-group-id',
                })
                await step(createInput({ headers: createHeaders(), message: createMessage() }))

                expect(mockIngestionLagHistogram.labels).not.toHaveBeenCalled()
            })

            it('should not observe histogram when message.partition is undefined', async () => {
                const step = createEmitEventStep({
                    outputs: createOutputs(),
                    groupId: 'test-group-id',
                })
                await step(
                    createInput({
                        headers: createHeaders({ now: new Date(FAKE_NOW_MS - 1000) }),
                        message: createMessage({ partition: undefined as unknown as number }),
                    })
                )

                expect(mockIngestionLagHistogram.labels).not.toHaveBeenCalled()
            })

            it('should handle partition 0 correctly in histogram', async () => {
                const step = createEmitEventStep({
                    outputs: createOutputs(),
                    groupId: 'test-group-id',
                })
                await step(
                    createInput({
                        headers: createHeaders({ now: new Date(FAKE_NOW_MS - 2500) }),
                        message: createMessage({ partition: 0 }),
                    })
                )

                expect(mockIngestionLagHistogram.labels).toHaveBeenCalledWith({
                    groupId: 'test-group-id',
                    partition: '0',
                })
                expect(mockObserveFn).toHaveBeenCalledWith(2500)
            })
        })
    })
})
