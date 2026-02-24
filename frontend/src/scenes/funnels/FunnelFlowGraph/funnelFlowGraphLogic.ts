import { Edge, MarkerType, Node } from '@xyflow/react'
import ELK, { ElkExtendedEdge, ElkNode } from 'elkjs/lib/elk.bundled.js'
import { actions, connect, kea, key, listeners, path, props, reducers, selectors } from 'kea'
import { subscriptions } from 'kea-subscriptions'

import { keyForInsightLogicProps } from 'scenes/insights/sharedUtils'

import { PathsLink } from '~/queries/schema/schema-general'
import { InsightLogicProps } from '~/types'
import { FunnelStepWithConversionMetrics } from '~/types'

import { funnelDataLogic } from '../funnelDataLogic'
import type { funnelFlowGraphLogicType } from './funnelFlowGraphLogicType'
import {
    bridgeConfigForExpansion,
    buildPathFlowElements,
    PathExpansion,
    pathExpansionCacheKey,
    PathFlowEdgeData,
    PATH_NODE_HEIGHT,
    PATH_NODE_WIDTH,
    PathFlowNodeData,
} from './pathFlowUtils'

export const NODE_HEIGHT = 160
export const NODE_WIDTH = 300
export const ELK_OPTIONS = {
    'elk.algorithm': 'layered',
    'elk.direction': 'RIGHT',
    'elk.layered.spacing.nodeNodeBetweenLayers': '140',
    'elk.spacing.nodeNode': '40',
    'elk.spacing.edgeEdge': '30',
    'elk.spacing.edgeNode': '30',
    'elk.layered.nodePlacement.strategy': 'SIMPLE',
    'elk.layered.nodePlacement.bk.fixedAlignment': 'BALANCED',
    'elk.layered.crossingMinimization.strategy': 'LAYER_SWEEP',
    'elk.padding': '[left=0, top=0, right=0, bottom=0]',
}

export interface FunnelFlowNodeData extends Record<string, unknown> {
    step: FunnelStepWithConversionMetrics
    stepIndex: number
    isOptional: boolean
}

export interface FunnelFlowEdgeData extends Record<string, unknown> {
    step: FunnelStepWithConversionMetrics
    stepIndex: number
    edgeIndex: number
}

type AnyFlowNode = Node<FunnelFlowNodeData> | Node<PathFlowNodeData>

const elk = new ELK()

const DEFAULT_LOGIC_KEY = 'default_funnel_flow_graph'

async function layoutNodes(nodes: AnyFlowNode[], edges: Edge[]): Promise<AnyFlowNode[]> {
    if (nodes.length === 0) {
        return []
    }

    const graph: ElkNode = {
        id: 'root',
        layoutOptions: ELK_OPTIONS,
        children: nodes.map((node) => {
            const isPathNode = node.type === 'pathNode'
            return {
                id: node.id,
                width: isPathNode ? PATH_NODE_WIDTH : NODE_WIDTH,
                height: isPathNode ? PATH_NODE_HEIGHT : NODE_HEIGHT,
                ports: [
                    { id: `${node.id}-target`, properties: { side: 'WEST' } },
                    { id: `${node.id}-source`, properties: { side: 'EAST' } },
                ],
                properties: {
                    'org.eclipse.elk.portConstraints': 'FIXED_ORDER',
                },
            }
        }),
        edges: edges.map((edge) => ({
            id: edge.id,
            sources: [edge.sourceHandle || edge.source],
            targets: [edge.targetHandle || edge.target],
        })) as ElkExtendedEdge[],
    }

    const laidOutGraph = await elk.layout(graph)
    const positionMap = new Map<string, { x: number; y: number }>()
    for (const child of laidOutGraph.children ?? []) {
        positionMap.set(child.id, { x: child.x ?? 0, y: child.y ?? 0 })
    }

    return nodes.map((node) => ({
        ...node,
        position: positionMap.get(node.id) ?? { x: 0, y: 0 },
    }))
}

export const funnelFlowGraphLogic = kea<funnelFlowGraphLogicType>([
    path((key) => ['scenes', 'funnels', 'FunnelFlowGraph', 'funnelFlowGraphLogic', key]),
    props({} as InsightLogicProps),
    key(keyForInsightLogicProps(DEFAULT_LOGIC_KEY)),

    connect((props: InsightLogicProps) => ({
        values: [funnelDataLogic(props), ['visibleStepsWithConversionMetrics', 'isStepOptional', 'querySource']],
    })),

    actions({
        setLaidOutNodes: (laidOutNodes: AnyFlowNode[]) => ({ laidOutNodes }),
        expandPath: (expansion: PathExpansion) => ({ expansion }),
        collapsePath: true,
        setPathsResults: (cacheKey: string, results: PathsLink[]) => ({ cacheKey, results }),
    }),

    reducers({
        laidOutNodes: [
            [] as AnyFlowNode[],
            {
                setLaidOutNodes: (_, { laidOutNodes }) => laidOutNodes,
            },
        ],
        expandedPath: [
            null as PathExpansion | null,
            {
                expandPath: (_, { expansion }) => expansion,
                collapsePath: () => null,
            },
        ],
        pathsResultsCache: [
            {} as Record<string, PathsLink[]>,
            {
                setPathsResults: (state, { cacheKey, results }) => ({ ...state, [cacheKey]: results }),
            },
        ],
        pathsLoading: [
            false,
            {
                expandPath: () => true,
                setPathsResults: () => false,
                collapsePath: () => false,
            },
        ],
    }),

    selectors({
        funnelNodes: [
            (s) => [s.visibleStepsWithConversionMetrics, s.isStepOptional],
            (steps, isStepOptional): Node<FunnelFlowNodeData>[] =>
                steps.map((step, index) => {
                    const optional = isStepOptional(index + 1)
                    return {
                        id: `step-${index}`,
                        type: optional ? 'optional' : 'mandatory',
                        data: { step, stepIndex: index, isOptional: optional },
                        position: { x: 0, y: 0 },
                        width: NODE_WIDTH,
                        height: NODE_HEIGHT,
                        draggable: false,
                        connectable: false,
                    }
                }),
        ],
        funnelEdges: [
            (s) => [s.funnelNodes],
            (nodes): Edge<FunnelFlowEdgeData>[] =>
                nodes.slice(0, -1).map((node, index) => {
                    const targetNode = nodes[index + 1]
                    const touchesOptionalStep = targetNode.data.isOptional
                    return {
                        id: `edge-${index}`,
                        source: node.id,
                        target: targetNode.id,
                        type: 'funnelFlow',
                        sourceHandle: `${node.id}-source`,
                        targetHandle: `${targetNode.id}-target`,
                        markerEnd: { type: MarkerType.ArrowClosed },
                        deletable: false,
                        style: touchesOptionalStep ? { strokeDasharray: '5 5' } : undefined,
                        data: {
                            step: targetNode.data.step,
                            stepIndex: targetNode.data.stepIndex,
                            edgeIndex: index,
                        },
                    }
                }),
        ],
        expandedPathCacheKey: [
            (s) => [s.expandedPath],
            (expandedPath): string | null => (expandedPath ? pathExpansionCacheKey(expandedPath) : null),
        ],
        expandedPathResults: [
            (s) => [s.expandedPathCacheKey, s.pathsResultsCache],
            (cacheKey, pathsResultsCache): PathsLink[] | null =>
                cacheKey ? (pathsResultsCache[cacheKey] ?? null) : null,
        ],
        expandedPathElements: [
            (s) => [s.funnelNodes, s.expandedPath, s.expandedPathResults],
            (
                funnelNodes,
                expandedPath,
                expandedPathResults
            ): {
                nodes: Node<PathFlowNodeData>[]
                edges: Edge<PathFlowEdgeData>[]
                hiddenEdgeId: string | null
            } | null => {
                if (!expandedPath || !expandedPathResults) {
                    return null
                }
                const bridgeConfig = bridgeConfigForExpansion(expandedPath, funnelNodes.length)

                const funnelStepByEventName = new Map<string, string>()
                for (const node of funnelNodes) {
                    const eventName = node.data.step.name
                    if (!funnelStepByEventName.has(eventName)) {
                        funnelStepByEventName.set(eventName, node.id)
                    }
                }

                const { nodes, edges } = buildPathFlowElements(
                    expandedPathResults,
                    bridgeConfig.sourceStepId,
                    bridgeConfig.targetStepId,
                    bridgeConfig.isDropOff || undefined,
                    funnelStepByEventName
                )
                return { nodes, edges, hiddenEdgeId: bridgeConfig.hiddenEdgeId }
            },
        ],
        nodes: [
            (s) => [s.funnelNodes, s.expandedPathElements],
            (funnelNodes, expandedPathElements): AnyFlowNode[] => {
                if (!expandedPathElements) {
                    return funnelNodes
                }
                return [...funnelNodes, ...expandedPathElements.nodes]
            },
        ],
        edges: [
            (s) => [s.funnelEdges, s.expandedPathElements],
            (funnelEdges, expandedPathElements): Edge[] => {
                if (!expandedPathElements) {
                    return funnelEdges
                }
                const visibleFunnelEdges = expandedPathElements.hiddenEdgeId
                    ? funnelEdges.filter((e) => e.id !== expandedPathElements.hiddenEdgeId)
                    : funnelEdges
                return [...visibleFunnelEdges, ...expandedPathElements.edges]
            },
        ],
    }),

    listeners(({ actions, values }) => ({
        expandPath: () => {
            if (values.expandedPathCacheKey && values.expandedPathResults) {
                actions.setPathsResults(values.expandedPathCacheKey, values.expandedPathResults)
            }
        },
    })),

    subscriptions(({ actions, values }) => ({
        nodes: async () => {
            const positioned = await layoutNodes(values.nodes, values.edges)
            actions.setLaidOutNodes(positioned)
        },
    })),
])
