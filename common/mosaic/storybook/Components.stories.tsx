import type { Meta, StoryFn } from '@storybook/react'
import type { ReactElement } from 'react'
import { useState } from 'react'

import {
    Badge,
    Card,
    DataTable,
    type DataTableColumn,
    Link,
    Select,
    Stack,
    Tabs,
    TabsContent,
    TabsList,
    TabsTrigger,
    Tooltip,
} from '../src/index'
import { McpThemeDecorator } from './decorator'

const meta: Meta = {
    title: 'Mosaic/Components',
    decorators: [McpThemeDecorator],
}
export default meta

// ---------------------------------------------------------------------------
// Section helper
// ---------------------------------------------------------------------------
function Section({ title, children }: { title: string; children: React.ReactNode }): ReactElement {
    return (
        <Stack gap="sm">
            <span style={{ fontSize: '0.875rem', fontWeight: 600, color: 'var(--color-text-primary)' }}>{title}</span>
            {children}
        </Stack>
    )
}

// ===========================
// Base components
// ===========================

export const BadgeStory: StoryFn = () => (
    <Stack gap="md">
        <Section title="Variants">
            <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
                <Badge variant="success">Success</Badge>
                <Badge variant="danger">Danger</Badge>
                <Badge variant="warning">Warning</Badge>
                <Badge variant="info">Info</Badge>
                <Badge variant="neutral">Neutral</Badge>
            </div>
        </Section>
        <Section title="Sizes">
            <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
                <Badge variant="info" size="sm">
                    Small
                </Badge>
                <Badge variant="info" size="md">
                    Medium
                </Badge>
            </div>
        </Section>
    </Stack>
)
BadgeStory.storyName = 'Badge'

export const CardStory: StoryFn = () => (
    <Stack gap="md">
        {(['sm', 'md', 'lg'] as const).map((padding) => (
            <Card key={padding} padding={padding}>
                <span style={{ fontSize: '0.875rem', color: 'var(--color-text-secondary)' }}>
                    Card with <code>{padding}</code> padding
                </span>
            </Card>
        ))}
    </Stack>
)
CardStory.storyName = 'Card'

export const StackStory: StoryFn = () => {
    const box: React.CSSProperties = {
        padding: '0.5rem 0.75rem',
        borderRadius: '0.375rem',
        background: 'var(--color-background-info)',
        color: 'var(--color-text-info)',
        fontSize: '0.75rem',
        fontWeight: 500,
    }
    return (
        <Stack gap="md">
            <Section title="Column (default)">
                <Stack gap="sm">
                    <div style={box}>Item 1</div>
                    <div style={box}>Item 2</div>
                    <div style={box}>Item 3</div>
                </Stack>
            </Section>
            <Section title="Row">
                <Stack direction="row" gap="sm">
                    <div style={box}>Item 1</div>
                    <div style={box}>Item 2</div>
                    <div style={box}>Item 3</div>
                </Stack>
            </Section>
            <Section title="Row, space-between">
                <Stack direction="row" gap="sm" justify="between">
                    <div style={box}>Left</div>
                    <div style={box}>Right</div>
                </Stack>
            </Section>
        </Stack>
    )
}
StackStory.storyName = 'Stack'

export const LinkStory: StoryFn = () => (
    <Stack gap="sm">
        <Link href="https://posthog.com" external>
            External link
        </Link>
        <Link href="#">Internal link</Link>
        <Link href="https://posthog.com/docs" external className="text-xs">
            Small external link
        </Link>
    </Stack>
)
LinkStory.storyName = 'Link'

export const TooltipStory: StoryFn = () => (
    <Stack gap="md">
        <div style={{ display: 'flex', gap: '2rem', flexWrap: 'wrap', padding: '1.5rem 0' }}>
            <Tooltip content="This appears above" position="top" forceVisible>
                <span
                    style={{
                        fontSize: '0.875rem',
                        cursor: 'default',
                        borderBottom: '1px dashed var(--color-text-secondary)',
                    }}
                >
                    Top tooltip
                </span>
            </Tooltip>
            <Tooltip content="This appears below" position="bottom" forceVisible>
                <span
                    style={{
                        fontSize: '0.875rem',
                        cursor: 'default',
                        borderBottom: '1px dashed var(--color-text-secondary)',
                    }}
                >
                    Bottom tooltip
                </span>
            </Tooltip>
            <Tooltip content="Tooltips work on any element" forceVisible>
                <Badge variant="info">Hover me</Badge>
            </Tooltip>
        </div>
    </Stack>
)
TooltipStory.storyName = 'Tooltip'

export const SelectStory: StoryFn = () => {
    const [value, setValue] = useState('table')
    return (
        <Stack gap="md">
            <Section title="Default">
                <Select
                    value={value}
                    onChange={setValue}
                    options={[
                        { value: 'table', label: 'Table' },
                        { value: 'bar', label: 'Bar chart' },
                        { value: 'line', label: 'Line chart' },
                    ]}
                />
            </Section>
            <Section title="Small">
                <Select
                    value={value}
                    onChange={setValue}
                    size="sm"
                    options={[
                        { value: 'table', label: 'Table' },
                        { value: 'bar', label: 'Bar chart' },
                    ]}
                />
            </Section>
        </Stack>
    )
}
SelectStory.storyName = 'Select'

export const TabsStory: StoryFn = () => (
    <Card padding="none">
        <Tabs defaultValue="overview">
            <TabsList className="px-3">
                <TabsTrigger value="overview">Overview</TabsTrigger>
                <TabsTrigger value="details">Details</TabsTrigger>
                <TabsTrigger value="settings">Settings</TabsTrigger>
            </TabsList>
            <TabsContent value="overview" className="px-4 pb-4">
                <span style={{ fontSize: '0.875rem', color: 'var(--color-text-secondary)' }}>Overview content</span>
            </TabsContent>
            <TabsContent value="details" className="px-4 pb-4">
                <span style={{ fontSize: '0.875rem', color: 'var(--color-text-secondary)' }}>Details content</span>
            </TabsContent>
            <TabsContent value="settings" className="px-4 pb-4">
                <span style={{ fontSize: '0.875rem', color: 'var(--color-text-secondary)' }}>Settings content</span>
            </TabsContent>
        </Tabs>
    </Card>
)
TabsStory.storyName = 'Tabs'

// -- DataTable --

interface SampleRow {
    name: string
    country: string
    events: number
    revenue: number | null
    active: boolean
}

const sampleData: SampleRow[] = [
    { name: 'Acme Corp', country: 'US', events: 14280, revenue: 52000, active: true },
    { name: 'Globex', country: 'UK', events: 8930, revenue: 31500, active: true },
    { name: 'Initech', country: 'US', events: 6210, revenue: null, active: false },
    { name: 'Umbrella', country: 'JP', events: 22450, revenue: 89000, active: true },
    { name: 'Hooli', country: 'US', events: 3100, revenue: 12500, active: true },
    { name: 'Pied Piper', country: 'US', events: 45670, revenue: 120000, active: true },
    { name: 'Stark Ind.', country: 'US', events: 18900, revenue: 75000, active: false },
    { name: 'Wayne Ent.', country: 'US', events: 29300, revenue: 95000, active: true },
]

const sampleColumns: DataTableColumn<SampleRow>[] = [
    { key: 'name', header: 'Company', sortable: true },
    { key: 'country', header: 'Country', sortable: true },
    { key: 'events', header: 'Events', align: 'right', sortable: true },
    { key: 'revenue', header: 'Revenue', align: 'right', sortable: true },
    {
        key: 'active',
        header: 'Status',
        render: (row) => (
            <Badge variant={row.active ? 'success' : 'neutral'} size="sm">
                {row.active ? 'Active' : 'Inactive'}
            </Badge>
        ),
    },
]

export const DataTableStory: StoryFn = () => (
    <DataTable
        columns={sampleColumns}
        data={sampleData}
        pageSize={5}
        defaultSort={{ key: 'events', direction: 'desc' }}
    />
)
DataTableStory.storyName = 'DataTable'
