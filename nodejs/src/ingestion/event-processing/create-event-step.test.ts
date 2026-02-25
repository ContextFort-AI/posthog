import { DateTime } from 'luxon'
import { Message } from 'node-rdkafka'

import { createTestEventHeaders } from '../../../tests/helpers/event-headers'
import { createTestMessage } from '../../../tests/helpers/kafka-message'
import { Person, PersonMode, PreIngestionEvent, ProjectId, TimestampFormat } from '../../types'
import { castTimestampOrNow } from '../../utils/utils'
import { isOkResult } from '../pipelines/results'
import { CreateEventStepInput, createCreateEventStep } from './create-event-step'
import { EVENTS_OUTPUT } from './ingestion-outputs'

describe('create-event-step', () => {
    let mockPerson: Person
    let mockPreparedEvent: PreIngestionEvent
    let mockMessage: Message

    beforeEach(() => {
        mockMessage = createTestMessage()
        mockPerson = {
            team_id: 1,
            properties: { email: 'test@example.com', name: 'Test User' },
            uuid: 'person-uuid-123',
            created_at: DateTime.fromISO('2023-01-01T00:00:00.000Z'),
            force_upgrade: false,
        }

        mockPreparedEvent = {
            eventUuid: 'event-uuid-456',
            event: '$pageview',
            teamId: 1,
            projectId: 1 as ProjectId,
            distinctId: 'distinct-id-789',
            properties: { $current_url: 'https://example.com' },
            timestamp: castTimestampOrNow('2023-01-01T00:00:00.000Z', TimestampFormat.ISO),
        }
    })

    describe('createCreateEventStep', () => {
        it('should create event with processPerson=true', async () => {
            const step = createCreateEventStep(EVENTS_OUTPUT)
            const input = {
                person: mockPerson,
                preparedEvent: mockPreparedEvent,
                processPerson: true,
                historicalMigration: false,
                headers: createTestEventHeaders(),
                message: mockMessage,
                lastStep: 'prepareEventStep',
            }

            const result = await step(input)

            expect(isOkResult(result)).toBe(true)
            if (isOkResult(result)) {
                const value = result.value
                expect(value.eventsToEmit).toHaveLength(1)
                expect(value.eventsToEmit[0].output).toBe(EVENTS_OUTPUT)
                const eventToEmit = value.eventsToEmit[0].event
                expect(eventToEmit.uuid).toBe('event-uuid-456')
                expect(eventToEmit.event).toBe('$pageview')
                expect(eventToEmit.team_id).toBe(1)
                expect(eventToEmit.distinct_id).toBe('distinct-id-789')
                expect(eventToEmit.person_id).toBe('person-uuid-123')
                expect(eventToEmit.person_mode).toBe('full')
                expect(eventToEmit.person_properties).toEqual({
                    email: 'test@example.com',
                    name: 'Test User',
                })
            }
            expect(result.sideEffects).toHaveLength(0)
        })

        it('should create event with processPerson=false', async () => {
            const step = createCreateEventStep(EVENTS_OUTPUT)
            const input = {
                person: mockPerson,
                preparedEvent: mockPreparedEvent,
                processPerson: false,
                historicalMigration: false,
                headers: createTestEventHeaders(),
                message: mockMessage,
                lastStep: 'prepareEventStep',
            }

            const result = await step(input)

            expect(isOkResult(result)).toBe(true)
            if (isOkResult(result)) {
                const value = result.value
                expect(value.eventsToEmit).toHaveLength(1)
                const eventToEmit = value.eventsToEmit[0].event
                expect(eventToEmit.person_mode).toBe('propertyless')
                expect(eventToEmit.person_properties).toEqual({})
            }
            expect(result.sideEffects).toHaveLength(0)
        })

        it('should handle person with force_upgrade=true', async () => {
            const personWithForceUpgrade: Person = {
                ...mockPerson,
                force_upgrade: true,
            }

            const step = createCreateEventStep(EVENTS_OUTPUT)
            const input = {
                person: personWithForceUpgrade,
                preparedEvent: mockPreparedEvent,
                processPerson: true,
                historicalMigration: false,
                headers: createTestEventHeaders(),
                message: mockMessage,
                lastStep: 'prepareEventStep',
            }

            const result = await step(input)

            expect(isOkResult(result)).toBe(true)
            if (isOkResult(result)) {
                const value = result.value
                expect(value.eventsToEmit).toHaveLength(1)
                const eventToEmit = value.eventsToEmit[0].event
                expect(eventToEmit.person_mode).toBe('force_upgrade')
            }
        })

        it('should include $set properties in person_properties when processPerson=true', async () => {
            const eventWithSetProperties: PreIngestionEvent = {
                ...mockPreparedEvent,
                properties: {
                    ...mockPreparedEvent.properties,
                    $set: { new_property: 'new_value' },
                },
            }

            const step = createCreateEventStep(EVENTS_OUTPUT)
            const input = {
                person: mockPerson,
                preparedEvent: eventWithSetProperties,
                processPerson: true,
                historicalMigration: false,
                headers: createTestEventHeaders(),
                message: mockMessage,
                lastStep: 'prepareEventStep',
            }

            const result = await step(input)

            expect(isOkResult(result)).toBe(true)
            if (isOkResult(result)) {
                const value = result.value
                expect(value.eventsToEmit).toHaveLength(1)
                const eventToEmit = value.eventsToEmit[0].event
                expect(eventToEmit.person_properties).toEqual({
                    email: 'test@example.com',
                    name: 'Test User',
                    new_property: 'new_value',
                })
            }
        })

        it('should preserve event properties as object', async () => {
            const step = createCreateEventStep(EVENTS_OUTPUT)
            const input = {
                person: mockPerson,
                preparedEvent: mockPreparedEvent,
                processPerson: true,
                historicalMigration: false,
                headers: createTestEventHeaders(),
                message: mockMessage,
                lastStep: 'prepareEventStep',
            }

            const result = await step(input)

            expect(isOkResult(result)).toBe(true)
            if (isOkResult(result)) {
                const value = result.value
                expect(value.eventsToEmit).toHaveLength(1)
                const eventToEmit = value.eventsToEmit[0].event
                expect(eventToEmit.properties).toEqual({
                    $current_url: 'https://example.com',
                })
            }
        })

        it('should handle events with elements_chain', async () => {
            const eventWithElements: PreIngestionEvent = {
                ...mockPreparedEvent,
                properties: {
                    ...mockPreparedEvent.properties,
                    $elements_chain: 'button:0;div:1;body:2',
                },
            }

            const step = createCreateEventStep(EVENTS_OUTPUT)
            const input = {
                person: mockPerson,
                preparedEvent: eventWithElements,
                processPerson: true,
                historicalMigration: false,
                headers: createTestEventHeaders(),
                message: mockMessage,
                lastStep: 'prepareEventStep',
            }

            const result = await step(input)

            expect(isOkResult(result)).toBe(true)
            if (isOkResult(result)) {
                const value = result.value
                expect(value.eventsToEmit).toHaveLength(1)
                const eventToEmit = value.eventsToEmit[0].event
                expect(eventToEmit.elements_chain).toBe('button:0;div:1;body:2')
            }
        })

        it('should work with generic input types that have required properties', async () => {
            interface CustomInput extends CreateEventStepInput {
                customProperty: string
                lastStep: string
            }

            const step = createCreateEventStep<typeof EVENTS_OUTPUT, CustomInput>(EVENTS_OUTPUT)
            const input: CustomInput = {
                person: mockPerson,
                preparedEvent: mockPreparedEvent,
                processPerson: true,
                historicalMigration: false,
                headers: createTestEventHeaders(),
                message: mockMessage,
                customProperty: 'test',
                lastStep: 'prepareEventStep',
            }

            const result = await step(input)

            expect(isOkResult(result)).toBe(true)
            if (isOkResult(result)) {
                expect(result.value.eventsToEmit).toHaveLength(1)
            }
        })

        it('should set correct timestamps', async () => {
            const step = createCreateEventStep(EVENTS_OUTPUT)
            const input = {
                person: mockPerson,
                preparedEvent: mockPreparedEvent,
                processPerson: true,
                historicalMigration: false,
                headers: createTestEventHeaders(),
                message: mockMessage,
                lastStep: 'prepareEventStep',
            }

            const result = await step(input)

            expect(isOkResult(result)).toBe(true)
            if (isOkResult(result)) {
                const value = result.value
                expect(value.eventsToEmit).toHaveLength(1)
                const eventToEmit = value.eventsToEmit[0].event
                expect(eventToEmit.timestamp).toBeTruthy()
                expect(eventToEmit.created_at).toBeNull()
                expect(eventToEmit.person_created_at).toBeInstanceOf(DateTime)
            }
        })

        it('should handle different event types', async () => {
            const events = ['$pageview', '$identify', '$set', 'custom_event']

            for (const eventName of events) {
                const eventWithType: PreIngestionEvent = {
                    ...mockPreparedEvent,
                    event: eventName,
                }

                const step = createCreateEventStep(EVENTS_OUTPUT)
                const input = {
                    person: mockPerson,
                    preparedEvent: eventWithType,
                    processPerson: true,
                    historicalMigration: false,
                    headers: createTestEventHeaders(),
                    message: mockMessage,
                    lastStep: 'prepareEventStep',
                }

                const result = await step(input)

                expect(isOkResult(result)).toBe(true)
                if (isOkResult(result)) {
                    expect(result.value.eventsToEmit).toHaveLength(1)
                    expect(result.value.eventsToEmit[0].event.event).toBe(eventName)
                }
            }
        })

        describe('historicalMigration flag', () => {
            it('should include historical_migration in event when historicalMigration=true', async () => {
                const step = createCreateEventStep(EVENTS_OUTPUT)
                const input = {
                    person: mockPerson,
                    preparedEvent: mockPreparedEvent,
                    processPerson: true,
                    historicalMigration: true,
                    headers: createTestEventHeaders(),
                    message: mockMessage,
                    lastStep: 'prepareEventStep',
                }

                const result = await step(input)

                expect(isOkResult(result)).toBe(true)
                if (isOkResult(result)) {
                    expect(result.value.eventsToEmit).toHaveLength(1)
                    expect(result.value.eventsToEmit[0].event.historical_migration).toBe(true)
                }
            })

            it('should not include historical_migration in event when historicalMigration=false', async () => {
                const step = createCreateEventStep(EVENTS_OUTPUT)
                const input = {
                    person: mockPerson,
                    preparedEvent: mockPreparedEvent,
                    processPerson: true,
                    historicalMigration: false,
                    headers: createTestEventHeaders(),
                    message: mockMessage,
                    lastStep: 'prepareEventStep',
                }

                const result = await step(input)

                expect(isOkResult(result)).toBe(true)
                if (isOkResult(result)) {
                    expect(result.value.eventsToEmit).toHaveLength(1)
                    expect(result.value.eventsToEmit[0].event.historical_migration).toBeUndefined()
                }
            })
        })

        describe('person modes', () => {
            it.each([
                ['full', { processPerson: true, force_upgrade: false }, 'full' as PersonMode],
                ['propertyless', { processPerson: false, force_upgrade: false }, 'propertyless' as PersonMode],
                ['force_upgrade', { processPerson: true, force_upgrade: true }, 'force_upgrade' as PersonMode],
            ])('should set person_mode=%s when processPerson=%p and force_upgrade=%p', async (_, config, expected) => {
                const person: Person = {
                    ...mockPerson,
                    force_upgrade: config.force_upgrade,
                }

                const step = createCreateEventStep(EVENTS_OUTPUT)
                const input = {
                    person,
                    preparedEvent: mockPreparedEvent,
                    processPerson: config.processPerson,
                    historicalMigration: false,
                    headers: createTestEventHeaders(),
                    message: mockMessage,
                    lastStep: 'prepareEventStep',
                }

                const result = await step(input)

                expect(isOkResult(result)).toBe(true)
                if (isOkResult(result)) {
                    expect(result.value.eventsToEmit).toHaveLength(1)
                    expect(result.value.eventsToEmit[0].event.person_mode).toBe(expected)
                }
            })
        })
    })
})
