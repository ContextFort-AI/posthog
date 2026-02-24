import { PersonMode, ProjectId, RawKafkaEvent } from '../../types'
import { parseJSON } from '../../utils/json-parse'
import { isOkResult } from '../pipelines/results'
import { createSplitAiEventsStep } from './split-ai-events-step'

function createRawKafkaEvent(
    properties: Record<string, unknown> = {},
    overrides: Partial<RawKafkaEvent> = {}
): RawKafkaEvent {
    return {
        uuid: 'event-uuid-123',
        event: '$ai_generation',
        team_id: 1,
        project_id: 1 as ProjectId,
        distinct_id: 'user-1',
        timestamp: '2023-01-01 00:00:00.000' as any,
        created_at: '2023-01-01 00:00:00.000' as any,
        elements_chain: '',
        person_mode: 'full' as PersonMode,
        properties: JSON.stringify(properties),
        ...overrides,
    }
}

describe('split-ai-events-step', () => {
    const mainTopic = 'clickhouse_events_json'
    const aiEventsTopic = 'clickhouse_ai_events_json'
    const step = createSplitAiEventsStep({ aiEventsTopic })

    it('should split an event with large AI properties into stripped + full', async () => {
        const event = createRawKafkaEvent({
            $ai_input: 'large input',
            $ai_output: 'large output',
            $ai_model: 'gpt-4',
            $browser: 'Chrome',
        })

        const result = await step({ eventsToEmit: [{ event, topic: mainTopic }] })
        expect(isOkResult(result)).toBe(true)
        if (!isOkResult(result)) {
            return
        }

        const { eventsToEmit } = result.value
        expect(eventsToEmit).toHaveLength(2)

        const [mainEntry, aiEntry] = eventsToEmit
        expect(mainEntry.topic).toBe(mainTopic)
        expect(aiEntry.topic).toBe(aiEventsTopic)

        expect(parseJSON(mainEntry.event.properties!)).toEqual({ $ai_model: 'gpt-4', $browser: 'Chrome' })
        expect(parseJSON(aiEntry.event.properties!)).toEqual({
            $ai_input: 'large input',
            $ai_output: 'large output',
            $ai_model: 'gpt-4',
            $browser: 'Chrome',
        })
    })

    it.each(['$ai_input', '$ai_output', '$ai_output_choices', '$ai_input_state', '$ai_output_state', '$ai_tools'])(
        'should strip %s from the main topic event',
        async (property) => {
            const event = createRawKafkaEvent({ [property]: 'large value', $ai_model: 'gpt-4' })

            const result = await step({ eventsToEmit: [{ event, topic: mainTopic }] })
            expect(isOkResult(result)).toBe(true)
            if (!isOkResult(result)) {
                return
            }

            const { eventsToEmit } = result.value
            expect(eventsToEmit).toHaveLength(2)

            expect(parseJSON(eventsToEmit[0].event.properties!)).not.toHaveProperty(property)
            expect(parseJSON(eventsToEmit[1].event.properties!)).toHaveProperty(property, 'large value')
        }
    )

    it('should pass through event without large AI properties', async () => {
        const event = createRawKafkaEvent({ $ai_model: 'gpt-4', $browser: 'Chrome' })

        const result = await step({ eventsToEmit: [{ event, topic: mainTopic }] })
        expect(isOkResult(result)).toBe(true)
        if (!isOkResult(result)) {
            return
        }

        const { eventsToEmit } = result.value
        expect(eventsToEmit).toHaveLength(1)
        expect(eventsToEmit[0].event).toBe(event)
        expect(eventsToEmit[0].topic).toBe(mainTopic)
    })

    it('should skip events already destined for the AI topic', async () => {
        const event = createRawKafkaEvent({ $ai_input: 'large input' })

        const result = await step({ eventsToEmit: [{ event, topic: aiEventsTopic }] })
        expect(isOkResult(result)).toBe(true)
        if (!isOkResult(result)) {
            return
        }

        const { eventsToEmit } = result.value
        expect(eventsToEmit).toHaveLength(1)
        expect(eventsToEmit[0].event).toBe(event)
        expect(eventsToEmit[0].topic).toBe(aiEventsTopic)
    })

    it('should handle multiple events independently', async () => {
        const aiEvent = createRawKafkaEvent({ $ai_input: 'large', $ai_model: 'gpt-4' }, { uuid: 'ai-1' })
        const regularEvent = createRawKafkaEvent({ $browser: 'Chrome' }, { uuid: 'regular-1', event: '$pageview' })

        const result = await step({
            eventsToEmit: [
                { event: aiEvent, topic: mainTopic },
                { event: regularEvent, topic: mainTopic },
            ],
        })
        expect(isOkResult(result)).toBe(true)
        if (!isOkResult(result)) {
            return
        }

        const { eventsToEmit } = result.value
        expect(eventsToEmit).toHaveLength(3)

        expect(eventsToEmit[0].topic).toBe(mainTopic)
        expect(parseJSON(eventsToEmit[0].event.properties!)).not.toHaveProperty('$ai_input')
        expect(eventsToEmit[1].topic).toBe(aiEventsTopic)
        expect(parseJSON(eventsToEmit[1].event.properties!)).toHaveProperty('$ai_input', 'large')
        expect(eventsToEmit[2].event).toBe(regularEvent)
        expect(eventsToEmit[2].topic).toBe(mainTopic)
    })

    it('should handle empty properties', async () => {
        const event = createRawKafkaEvent({})

        const result = await step({ eventsToEmit: [{ event, topic: mainTopic }] })
        expect(isOkResult(result)).toBe(true)
        if (!isOkResult(result)) {
            return
        }

        expect(result.value.eventsToEmit).toHaveLength(1)
        expect(result.value.eventsToEmit[0].event).toBe(event)
    })

    it('should handle undefined properties', async () => {
        const event = createRawKafkaEvent()
        event.properties = undefined

        const result = await step({ eventsToEmit: [{ event, topic: mainTopic }] })
        expect(isOkResult(result)).toBe(true)
        if (!isOkResult(result)) {
            return
        }

        expect(result.value.eventsToEmit).toHaveLength(1)
        expect(result.value.eventsToEmit[0].event).toBe(event)
    })

    it('should handle empty input', async () => {
        const result = await step({ eventsToEmit: [] })
        expect(isOkResult(result)).toBe(true)
        if (!isOkResult(result)) {
            return
        }

        expect(result.value.eventsToEmit).toHaveLength(0)
    })

    it('should copy for main topic and keep original for AI topic', async () => {
        const event = createRawKafkaEvent({ $ai_input: 'large input', $ai_model: 'gpt-4' })

        const result = await step({ eventsToEmit: [{ event, topic: mainTopic }] })
        expect(isOkResult(result)).toBe(true)
        if (!isOkResult(result)) {
            return
        }

        const { eventsToEmit } = result.value
        expect(eventsToEmit).toHaveLength(2)

        // Main topic entry is a copy with stripped properties
        expect(eventsToEmit[0].event).not.toBe(event)
        expect(parseJSON(eventsToEmit[0].event.properties!)).not.toHaveProperty('$ai_input')

        // AI topic entry keeps the original event object
        expect(eventsToEmit[1].event).toBe(event)
    })
})
