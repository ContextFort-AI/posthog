import { PluginEvent } from '~/plugin-scaffold'

import { createTestPluginEvent } from '../../../tests/helpers/plugin-event'
import { processAiEvent } from '../ai'
import { PipelineResultType } from '../pipelines/results'
import { createProcessAiEventStep } from './process-ai-event-step'

jest.mock('../ai', () => ({
    ...jest.requireActual('../ai'),
    processAiEvent: jest.fn(),
}))

type TestInput = {
    event: PluginEvent
    extraField: string
}

describe('createProcessAiEventStep', () => {
    beforeEach(() => {
        jest.clearAllMocks()
    })

    const createInput = (overrides: Partial<TestInput> = {}): TestInput => ({
        event: createTestPluginEvent(),
        extraField: 'preserved',
        ...overrides,
    })

    it.each(['$pageview', '$autocapture', 'custom_event'])(
        'should pass through non-AI event %s unchanged',
        async (eventName) => {
            const input = createInput({ event: createTestPluginEvent({ event: eventName }) })
            const step = createProcessAiEventStep<TestInput>()

            const result = await step(input)

            expect(result.type).toBe(PipelineResultType.OK)
            if (result.type === PipelineResultType.OK) {
                expect(result.value.event).toBe(input.event)
                expect(result.value.extraField).toBe('preserved')
            }
            expect(processAiEvent).not.toHaveBeenCalled()
        }
    )

    it.each(['$ai_generation', '$ai_embedding', '$ai_span', '$ai_trace', '$ai_metric', '$ai_feedback'])(
        'should enrich AI event %s through processAiEvent',
        async (eventName) => {
            const aiEvent = createTestPluginEvent({ event: eventName })
            const enrichedEvent = createTestPluginEvent({ event: eventName, properties: { enriched: true } })
            jest.mocked(processAiEvent).mockReturnValue(enrichedEvent)

            const step = createProcessAiEventStep<TestInput>()
            const result = await step(createInput({ event: aiEvent }))

            expect(result.type).toBe(PipelineResultType.OK)
            if (result.type === PipelineResultType.OK) {
                expect(result.value.event).toBe(enrichedEvent)
                expect(result.value.extraField).toBe('preserved')
            }
            expect(processAiEvent).toHaveBeenCalledWith(aiEvent)
        }
    )

    it('should swallow processAiEvent errors and use original event', async () => {
        const aiEvent = createTestPluginEvent({ event: '$ai_generation' })
        jest.mocked(processAiEvent).mockImplementation(() => {
            throw new Error('AI processing failed')
        })

        const step = createProcessAiEventStep<TestInput>()
        const result = await step(createInput({ event: aiEvent }))

        expect(result.type).toBe(PipelineResultType.OK)
        if (result.type === PipelineResultType.OK) {
            expect(result.value.event).toBe(aiEvent)
        }
    })
})
