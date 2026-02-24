import '@xyflow/react/dist/style.css'

import {
    Background,
    BackgroundVariant,
    Controls,
    EdgeTypes,
    MiniMap,
    NodeTypes,
    ReactFlow,
    ReactFlowProvider,
} from '@xyflow/react'
import { useActions, useValues } from 'kea'
import { useCallback, useEffect, useMemo, useRef } from 'react'

import { insightLogic } from 'scenes/insights/insightLogic'

import { themeLogic } from '~/layout/navigation-3000/themeLogic'
import { DataNodeLogicProps, dataNodeLogic } from '~/queries/nodes/DataNode/dataNodeLogic'
import { PathsLink } from '~/queries/schema/schema-general'

import { funnelDataLogic } from '../funnelDataLogic'
import { FunnelFlowEdge } from './FunnelFlowEdge'
import { funnelFlowGraphLogic } from './funnelFlowGraphLogic'
import { FunnelFlowNode } from './FunnelFlowNode'
import { PathFlowEdge } from './PathFlowEdge'
import { PathFlowNode } from './PathFlowNode'
import { buildPathsQuery, PathExpansion } from './pathFlowUtils'

const NODE_TYPES = {
    mandatory: FunnelFlowNode,
    optional: FunnelFlowNode,
    pathNode: PathFlowNode,
} as NodeTypes

const EDGE_TYPES = {
    funnelFlow: FunnelFlowEdge,
    pathFlow: PathFlowEdge,
} as EdgeTypes

const FIT_VIEW_OPTIONS = {
    padding: 0.2,
    maxZoom: 1,
}

function PathsQueryExecutor({ expansion, cacheKey }: { expansion: PathExpansion; cacheKey: string }): null {
    const { insightProps } = useValues(insightLogic)
    const { querySource } = useValues(funnelDataLogic(insightProps))
    const { setPathsResults, collapsePath } = useActions(funnelFlowGraphLogic(insightProps))

    const pathsQuery = useMemo(
        () => (querySource ? buildPathsQuery(expansion, querySource) : null),
        [expansion, querySource]
    )

    const dataNodeProps = useMemo<DataNodeLogicProps>(
        () => ({
            key: `funnel-paths-${cacheKey}`,
            query: pathsQuery!,
        }),
        [cacheKey, pathsQuery]
    )

    const { response, responseErrorObject } = useValues(dataNodeLogic(dataNodeProps))

    const handledRef = useRef<unknown>(null)

    useEffect(() => {
        if (response?.results && response !== handledRef.current) {
            handledRef.current = response
            const results = (response.results ?? []) as PathsLink[]
            if (results.length === 0) {
                collapsePath()
            } else {
                setPathsResults(cacheKey, results)
            }
        }
    }, [response, cacheKey, setPathsResults, collapsePath])

    useEffect(() => {
        if (responseErrorObject && responseErrorObject !== handledRef.current) {
            handledRef.current = responseErrorObject
            collapsePath()
        }
    }, [responseErrorObject, collapsePath])

    return null
}

function PathsQueryRunner(): JSX.Element | null {
    const { insightProps } = useValues(insightLogic)
    const { expandedPath, expandedPathCacheKey, expandedPathResults, querySource } = useValues(
        funnelFlowGraphLogic(insightProps)
    )

    const needsQuery =
        expandedPath !== null && !expandedPathResults && querySource?.aggregation_group_type_index == undefined

    if (!needsQuery || expandedPath === null || expandedPathCacheKey === null) {
        return null
    }

    return <PathsQueryExecutor key={expandedPathCacheKey} expansion={expandedPath} cacheKey={expandedPathCacheKey} />
}

function FunnelFlowGraphContent(): JSX.Element {
    const { isDarkModeOn } = useValues(themeLogic)
    const { insightProps } = useValues(insightLogic)
    const { laidOutNodes, edges } = useValues(funnelFlowGraphLogic(insightProps))

    const closeOpenPopovers = useCallback(() => {
        document.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }))
    }, [])

    return (
        <div className="relative w-full" style={{ height: 'var(--insight-viz-min-height)' }}>
            {/* Raise edge labels above nodes so the expand/collapse button stays clickable */}
            <style>{'.react-flow__edgelabel-renderer { z-index: 5; }'}</style>
            <PathsQueryRunner />
            <ReactFlow
                nodes={laidOutNodes}
                edges={edges}
                nodeTypes={NODE_TYPES}
                edgeTypes={EDGE_TYPES}
                nodesDraggable={false}
                nodesConnectable={false}
                fitView
                fitViewOptions={FIT_VIEW_OPTIONS}
                colorMode={isDarkModeOn ? 'dark' : 'light'}
                proOptions={{ hideAttribution: true }}
                elevateNodesOnSelect={false}
                minZoom={0.25}
                maxZoom={1.5}
                onPaneClick={closeOpenPopovers}
                onNodeClick={closeOpenPopovers}
            >
                <Background gap={36} variant={BackgroundVariant.Dots} />
                <Controls showInteractive={false} fitViewOptions={FIT_VIEW_OPTIONS} />
                {laidOutNodes.length > 4 && (
                    <MiniMap
                        zoomable
                        pannable
                        nodeStrokeWidth={3}
                        nodeColor="var(--border)"
                        nodeStrokeColor="var(--border)"
                    />
                )}
            </ReactFlow>
        </div>
    )
}

export function FunnelFlowGraph(): JSX.Element {
    return (
        <ReactFlowProvider>
            <FunnelFlowGraphContent />
        </ReactFlowProvider>
    )
}
