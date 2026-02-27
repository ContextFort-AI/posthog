import type { Meta, StoryFn } from '@storybook/react'

import { type FeatureFlagData, FeatureFlagView } from '../src/index'
import { McpThemeDecorator } from './decorator'

const meta: Meta = {
    title: 'Mosaic/Feature Flags',
    decorators: [McpThemeDecorator],
}
export default meta

const sampleBooleanFlag: FeatureFlagData = {
    id: 1,
    key: 'enable-new-dashboard',
    name: 'New dashboard experience',
    description: 'Enables the redesigned dashboard for selected users.',
    active: true,
    filters: {
        groups: [
            {
                properties: [{ key: 'email', value: '@posthog.com', operator: 'icontains', type: 'person' }],
                rollout_percentage: 100,
            },
            {
                properties: [],
                rollout_percentage: 50,
            },
        ],
    },
    tags: ['frontend', 'experiment'],
    updated_at: '2025-12-15T14:30:00Z',
    _posthogUrl: 'https://us.posthog.com/project/1/feature_flags/1',
}

const sampleMultivariateFlag: FeatureFlagData = {
    id: 2,
    key: 'checkout-flow-variant',
    name: 'Checkout flow experiment',
    description: 'A/B/C test for the checkout experience.',
    active: true,
    filters: {
        groups: [
            {
                properties: [{ key: 'plan', value: 'enterprise', operator: 'exact', type: 'person' }],
                rollout_percentage: 100,
                variant: 'test-a',
            },
            {
                properties: [],
                rollout_percentage: 80,
            },
        ],
        multivariate: {
            variants: [
                { key: 'control', name: 'Current flow', rollout_percentage: 50 },
                { key: 'test-a', name: 'Streamlined', rollout_percentage: 30 },
                { key: 'test-b', name: 'One-page', rollout_percentage: 20 },
            ],
        },
    },
    tags: ['checkout', 'growth'],
    updated_at: '2025-12-20T09:00:00Z',
    _posthogUrl: 'https://us.posthog.com/project/1/feature_flags/2',
}

const sampleInactiveFlag: FeatureFlagData = {
    id: 3,
    key: 'deprecated-feature',
    name: 'Old feature toggle',
    active: false,
    filters: {
        groups: [
            {
                properties: [],
                rollout_percentage: 0,
            },
        ],
    },
    updated_at: '2024-06-01T00:00:00Z',
}

export const BooleanFlag: StoryFn = () => <FeatureFlagView flag={sampleBooleanFlag} />
BooleanFlag.storyName = 'Boolean flag'

export const MultivariateFlag: StoryFn = () => <FeatureFlagView flag={sampleMultivariateFlag} />
MultivariateFlag.storyName = 'Multivariate flag with variant override'

export const InactiveFlag: StoryFn = () => <FeatureFlagView flag={sampleInactiveFlag} />
InactiveFlag.storyName = 'Inactive flag'
