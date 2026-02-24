import { Message } from 'node-rdkafka'

import { HogTransformerService } from '../../cdp/hog-transformations/hog-transformer.service'
import { KafkaProducerWrapper } from '../../kafka/producer'
import { Team } from '../../types'
import { TeamManager } from '../../utils/team-manager'
import { GroupTypeManager } from '../../worker/ingestion/group-type-manager'
import { BatchWritingGroupStore } from '../../worker/ingestion/groups/batch-writing-group-store'
import { PersonsStore } from '../../worker/ingestion/persons/persons-store'
import { AI_EVENT_TYPES } from '../ai'
import { createAiEventSubpipeline } from '../ai/pipelines/ai-event-subpipeline'
import { EventPipelineRunnerOptions } from '../event-processing/event-pipeline-options'
import { AiEventOutput, IngestionOutputs } from '../event-processing/ingestion-outputs'
import { PipelineBuilder, StartPipelineBuilder } from '../pipelines/builders/pipeline-builders'
import { TopHogWrapper } from '../pipelines/extensions/tophog'
import {
    ClientIngestionWarningSubpipelineInput,
    createClientIngestionWarningSubpipeline,
} from './client-ingestion-warning-subpipeline'
import { EventSubpipelineInput, createEventSubpipeline } from './event-subpipeline'
import { HeatmapSubpipelineInput, createHeatmapSubpipeline } from './heatmap-subpipeline'

export type PerDistinctIdPipelineInput = EventSubpipelineInput &
    HeatmapSubpipelineInput &
    ClientIngestionWarningSubpipelineInput

export interface PerDistinctIdPipelineConfig {
    options: EventPipelineRunnerOptions & {
        CLICKHOUSE_HEATMAPS_KAFKA_TOPIC: string
    }
    teamManager: TeamManager
    groupTypeManager: GroupTypeManager
    hogTransformer: HogTransformerService
    personsStore: PersonsStore
    groupStore: BatchWritingGroupStore
    kafkaProducer: KafkaProducerWrapper
    outputs: IngestionOutputs<AiEventOutput>
    groupId: string
    topHog: TopHogWrapper
}

export interface PerDistinctIdPipelineContext {
    message: Message
    team: Team
}

type EventBranch = 'client_ingestion_warning' | 'heatmap' | 'ai' | 'event'

const EVENT_BRANCH_MAP = new Map<string, EventBranch>([
    ['$$client_ingestion_warning', 'client_ingestion_warning'],
    ['$$heatmap', 'heatmap'],
    ...[...AI_EVENT_TYPES].map((t) => [t, 'ai'] as const),
])

function classifyEvent(input: PerDistinctIdPipelineInput): EventBranch {
    return EVENT_BRANCH_MAP.get(input.event.event) ?? 'event'
}

export function createPerDistinctIdPipeline<TInput extends PerDistinctIdPipelineInput, TContext>(
    builder: StartPipelineBuilder<TInput, TContext>,
    config: PerDistinctIdPipelineConfig
): PipelineBuilder<TInput, void, TContext> {
    const {
        options,
        teamManager,
        groupTypeManager,
        hogTransformer,
        personsStore,
        groupStore,
        kafkaProducer,
        outputs,
        groupId,
        topHog,
    } = config

    return builder.retry(
        (e) =>
            e.branching<EventBranch, void>(classifyEvent, (branches) => {
                branches
                    .branch('client_ingestion_warning', (b) => createClientIngestionWarningSubpipeline(b))
                    .branch('heatmap', (b) =>
                        createHeatmapSubpipeline(b, {
                            options,
                            teamManager,
                            groupTypeManager,
                            groupStore,
                            kafkaProducer,
                        })
                    )
                    .branch('ai', (b) =>
                        createAiEventSubpipeline(b, {
                            options,
                            teamManager,
                            groupTypeManager,
                            hogTransformer,
                            personsStore,
                            groupStore,
                            kafkaProducer,
                            outputs,
                            groupId,
                            topHog,
                        })
                    )
                    .branch('event', (b) =>
                        createEventSubpipeline(b, {
                            options,
                            teamManager,
                            groupTypeManager,
                            hogTransformer,
                            personsStore,
                            groupStore,
                            kafkaProducer,
                            outputs,
                            groupId,
                            topHog,
                        })
                    )
            }),
        { tries: 3, sleepMs: 100 }
    )
}
