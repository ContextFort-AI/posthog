import { PluginEvent } from '~/plugin-scaffold'

import { logger } from '../../utils/logger'
import { captureException } from '../../utils/posthog'
import { AI_EVENT_TYPES, processAiEvent } from '../ai'
import { ok } from '../pipelines/results'
import { ProcessingStep } from '../pipelines/steps'

export type ProcessAiEventStepInput = {
    event: PluginEvent
}

export function createProcessAiEventStep<TInput extends ProcessAiEventStepInput>(): ProcessingStep<TInput, TInput> {
    return function processAiEventStep(input) {
        const { event } = input

        if (!AI_EVENT_TYPES.has(event.event)) {
            return Promise.resolve(ok(input))
        }

        try {
            return Promise.resolve(ok({ ...input, event: processAiEvent(event) }))
        } catch (error) {
            captureException(error)
            logger.error(error)
            return Promise.resolve(ok(input))
        }
    }
}
