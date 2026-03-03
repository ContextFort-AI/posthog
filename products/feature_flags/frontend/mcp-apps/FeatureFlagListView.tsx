import { type ReactElement, type ReactNode, useCallback, useState } from 'react'

import { Badge, DataTable, type DataTableColumn, Stack } from '@posthog/mosaic'

import { FeatureFlagView, type FeatureFlagData } from './FeatureFlagView'

export interface FeatureFlagListData {
    count: number
    results: FeatureFlagData[]
    next: string | null
    previous: string | null
    _posthogUrl?: string
}

export interface FeatureFlagListViewProps {
    data: FeatureFlagListData
    onFlagClick?: (flag: FeatureFlagData) => Promise<FeatureFlagData | null>
}

type ViewState = { view: 'list' } | { view: 'loading'; flagKey: string } | { view: 'detail'; flag: FeatureFlagData }

function formatDate(iso: string): string {
    try {
        return new Date(iso).toLocaleDateString(undefined, {
            year: 'numeric',
            month: 'short',
            day: 'numeric',
        })
    } catch {
        return iso
    }
}

function BackButton({ onClick }: { onClick: () => void }): ReactElement {
    return (
        <button
            onClick={onClick}
            className="flex items-center gap-1 text-sm text-text-secondary hover:text-text-primary cursor-pointer transition-colors"
        >
            <span>&larr;</span>
            <span>All flags</span>
        </button>
    )
}

function LoadingState({ flagKey }: { flagKey: string }): ReactElement {
    return (
        <div className="flex flex-col items-center justify-center py-12 gap-3">
            <div className="h-5 w-5 rounded-full border-2 border-text-secondary border-t-transparent animate-spin" />
            <span className="text-sm text-text-secondary">Loading {flagKey}...</span>
        </div>
    )
}

export function FeatureFlagListView({ data, onFlagClick }: FeatureFlagListViewProps): ReactElement {
    const [viewState, setViewState] = useState<ViewState>({ view: 'list' })

    const handleFlagClick = useCallback(
        async (flag: FeatureFlagData) => {
            if (!onFlagClick) {
                return
            }
            setViewState({ view: 'loading', flagKey: flag.key })
            const detail = await onFlagClick(flag)
            if (detail) {
                setViewState({ view: 'detail', flag: detail })
            } else {
                setViewState({ view: 'list' })
            }
        },
        [onFlagClick]
    )

    const handleBack = useCallback(() => setViewState({ view: 'list' }), [])

    if (viewState.view === 'loading') {
        return (
            <div className="p-4">
                <Stack gap="sm">
                    <BackButton onClick={handleBack} />
                    <LoadingState flagKey={viewState.flagKey} />
                </Stack>
            </div>
        )
    }

    if (viewState.view === 'detail') {
        return (
            <div className="p-4">
                <Stack gap="sm">
                    <BackButton onClick={handleBack} />
                    <FeatureFlagView flag={viewState.flag} />
                </Stack>
            </div>
        )
    }

    const columns: DataTableColumn<FeatureFlagData>[] = [
        {
            key: 'key',
            header: 'Key',
            sortable: true,
            render: (row): ReactNode =>
                onFlagClick ? (
                    <button
                        onClick={() => handleFlagClick(row)}
                        className="text-link hover:underline cursor-pointer text-left"
                    >
                        {row.key}
                    </button>
                ) : (
                    row.key
                ),
        },
        {
            key: 'name',
            header: 'Name',
            sortable: true,
        },
        {
            key: 'active',
            header: 'Status',
            sortable: true,
            render: (row): ReactNode => (
                <Badge variant={row.active ? 'success' : 'neutral'} size="sm">
                    {row.active ? 'Active' : 'Inactive'}
                </Badge>
            ),
        },
        {
            key: 'tags',
            header: 'Tags',
            render: (row): ReactNode =>
                row.tags?.length ? (
                    <div className="flex gap-1 flex-wrap">
                        {row.tags.map((tag) => (
                            <Badge key={tag} variant="neutral" size="sm">
                                {tag}
                            </Badge>
                        ))}
                    </div>
                ) : (
                    <span className="text-text-secondary">&mdash;</span>
                ),
        },
        {
            key: 'updated_at',
            header: 'Last updated',
            sortable: true,
            render: (row): ReactNode =>
                row.updated_at ? (
                    <span className="text-text-secondary">{formatDate(row.updated_at)}</span>
                ) : (
                    <span className="text-text-secondary">&mdash;</span>
                ),
        },
    ]

    return (
        <div className="p-4">
            <Stack gap="sm">
                <div className="flex items-center justify-between">
                    <span className="text-sm text-text-secondary">
                        {data.count} {data.count === 1 ? 'flag' : 'flags'}
                    </span>
                </div>
                <DataTable<FeatureFlagData>
                    columns={columns}
                    data={data.results}
                    pageSize={10}
                    defaultSort={{ key: 'key', direction: 'asc' }}
                    emptyMessage="No feature flags found"
                />
            </Stack>
        </div>
    )
}
