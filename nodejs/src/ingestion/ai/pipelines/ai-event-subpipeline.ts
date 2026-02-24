import { EventSubpipelineConfig, EventSubpipelineInput } from '../../analytics/event-subpipeline'
import { createCreateEventStep } from '../../event-processing/create-event-step'
import { createEmitEventStep } from '../../event-processing/emit-event-step'
import { createHogTransformEventStep } from '../../event-processing/hog-transform-event-step'
import { createNormalizeEventStep } from '../../event-processing/normalize-event-step'
import { createNormalizeProcessPersonFlagStep } from '../../event-processing/normalize-process-person-flag-step'
import { createPrepareEventStep } from '../../event-processing/prepare-event-step'
import { createProcessAiEventStep } from '../../event-processing/process-ai-event-step'
import { createProcessPersonlessStep } from '../../event-processing/process-personless-step'
import { createProcessPersonsStep } from '../../event-processing/process-persons-step'
import { createSplitAiEventsStep } from '../../event-processing/split-ai-events-step'
import { PipelineBuilder, StartPipelineBuilder } from '../../pipelines/builders/pipeline-builders'
import { sum } from '../../pipelines/extensions/tophog'

export function createAiEventSubpipeline<TInput extends EventSubpipelineInput, TContext>(
    builder: StartPipelineBuilder<TInput, TContext>,
    config: EventSubpipelineConfig
): PipelineBuilder<TInput, void, TContext> {
    const {
        options,
        teamManager,
        groupTypeManager,
        hogTransformer,
        personsStore,
        groupStore,
        kafkaProducer,
        groupId,
        topHog,
    } = config

    return builder
        .pipe(createNormalizeProcessPersonFlagStep())
        .pipe(createHogTransformEventStep(hogTransformer))
        .pipe(createNormalizeEventStep())
        .pipe(createProcessAiEventStep())
        .pipe(createProcessPersonlessStep(personsStore))
        .pipe(createProcessPersonsStep(options, kafkaProducer, personsStore))
        .pipe(createPrepareEventStep(kafkaProducer, teamManager, groupTypeManager, groupStore, options))
        .pipe(createCreateEventStep(options.CLICKHOUSE_JSON_EVENTS_KAFKA_TOPIC))
        .pipe(createSplitAiEventsStep({ aiEventsTopic: options.CLICKHOUSE_AI_EVENTS_KAFKA_TOPIC }))
        .pipe(
            topHog(
                createEmitEventStep({
                    kafkaProducer,
                    groupId,
                }),
                // team_id is the same for all events in the list
                [
                    sum(
                        'emitted_events',
                        (input) => ({ team_id: String(input.teamId) }),
                        (input) => input.eventsToEmit.length
                    ),
                ]
            )
        )
}
