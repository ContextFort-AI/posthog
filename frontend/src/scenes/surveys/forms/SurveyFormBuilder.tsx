import { BindLogic, useActions, useValues } from 'kea'
import { useRef } from 'react'

import {
    IconBook,
    IconDocument,
    IconGlobe,
    IconGridMasonry,
    IconPalette,
    IconQuestion,
    IconShuffle,
    IconTarget,
} from '@posthog/icons'
import { LemonButton } from '@posthog/lemon-ui'

import { SpinnerOverlay } from 'lib/lemon-ui/Spinner'
import { SceneExport } from 'scenes/sceneTypes'

import { KeyboardShortcut } from '~/layout/navigation-3000/components/KeyboardShortcut'
import { SceneContent } from '~/layout/scenes/components/SceneContent'
import { SceneTitleSection } from '~/layout/scenes/components/SceneTitleSection'

import { FormEditor, FormEditorHandle } from './components/editor/FormEditor'
import { SurveyCover } from './components/SurveyCover'
import { SurveyLogo } from './components/SurveyLogo'
import { SurveyFormBuilderLogicProps, surveyFormBuilderLogic } from './surveyFormBuilderLogic'

export const scene: SceneExport<SurveyFormBuilderLogicProps> = {
    component: SurveyFormBuilderComponent,
    paramsToProps: ({ params: { id } }): SurveyFormBuilderLogicProps => ({ id: id || 'new' }),
}

function SurveyFormBuilderComponent({ id }: SurveyFormBuilderLogicProps): JSX.Element {
    return (
        <BindLogic logic={surveyFormBuilderLogic} props={{ id }}>
            <SurveyFormBuilder />
        </BindLogic>
    )
}

function SurveyFormBuilder(): JSX.Element {
    const { surveyForm, surveyName, existingSurveyLoading, isSurveyFormSubmitting, surveyLaunching, hasRealContent } =
        useValues(surveyFormBuilderLogic)
    const { setSurveyFormValues, submitSurveyForm, launchSurvey, addLogo, toggleCover } =
        useActions(surveyFormBuilderLogic)
    const editorRef = useRef<FormEditorHandle>(null)

    if (existingSurveyLoading) {
        return <SpinnerOverlay />
    }

    return (
        <SceneContent className="min-h-full pb-4">
            <SceneTitleSection
                name={surveyName}
                resourceType={{
                    type: 'survey',
                }}
                actions={
                    <div className="flex items-center gap-2">
                        <LemonButton
                            type="secondary"
                            size="small"
                            loading={isSurveyFormSubmitting}
                            disabledReason={isSurveyFormSubmitting ? 'Saving...' : undefined}
                            onClick={submitSurveyForm}
                        >
                            Save
                        </LemonButton>
                        <LemonButton
                            type="primary"
                            size="small"
                            loading={surveyLaunching}
                            disabledReason={surveyLaunching ? 'Publishing...' : undefined}
                            onClick={launchSurvey}
                        >
                            Publish
                        </LemonButton>
                    </div>
                }
            />
            <div className="w-full flex-1 min-h-0 bg-bg-light rounded-lg border border-border shadow-sm overflow-hidden">
                {surveyForm.showCover && <SurveyCover />}
                {surveyForm.showLogo && <SurveyLogo showCover={surveyForm.showCover} />}
                <div
                    className={`mx-auto max-w-4xl px-4 ${surveyForm.showCover || surveyForm.showLogo ? 'pt-0' : 'py-12'} pb-12`}
                >
                    <FormEditor
                        ref={editorRef}
                        content={surveyForm.content}
                        onUpdate={(content) => setSurveyFormValues({ content })}
                        onAddLogo={addLogo}
                        onAddCover={toggleCover}
                        showLogo={surveyForm.showLogo}
                        showCover={surveyForm.showCover}
                    />
                    {!hasRealContent && (
                        <div className="flex flex-col gap-6 px-12 mt-2">
                            <div className="flex gap-1">
                                <LemonButton
                                    type="tertiary"
                                    size="xsmall"
                                    icon={<>↵</>}
                                    onClick={() => editorRef.current?.insertEmptyParagraph()}
                                >
                                    Press enter to start from scratch
                                </LemonButton>
                                <LemonButton type="tertiary" size="xsmall" icon={<IconGridMasonry />}>
                                    Use a template
                                </LemonButton>
                            </div>
                            <div className="flex flex-col gap-2 text-muted">
                                <p className="mb-0 text-muted">
                                    Build your form like you're{' '}
                                    <span className="bg-accent-highlight-secondary p-0.75 rounded font-medium">
                                        writing a doc.
                                    </span>{' '}
                                </p>
                                <p className="mb-0 text-muted">
                                    Just type{' '}
                                    <KeyboardShortcut className="bg-accent-highlight-secondary" forwardslash /> to
                                    insert form blocks and question fields.
                                </p>
                            </div>
                            <div className="grid grid-cols-2 gap-8 mt-3">
                                <div className="flex flex-col gap-1">
                                    <span className="text-xs font-semibold uppercase tracking-wide text-muted mb-1">
                                        Get started
                                    </span>
                                    <LemonButton
                                        type="tertiary"
                                        size="xsmall"
                                        icon={<IconDocument />}
                                        to="https://posthog.com/docs/surveys"
                                        targetBlank
                                    >
                                        Create your first form
                                    </LemonButton>
                                    <LemonButton type="tertiary" size="xsmall" icon={<IconGridMasonry />}>
                                        Get started with templates
                                    </LemonButton>
                                    <LemonButton type="tertiary" size="xsmall" icon={<IconGlobe />}>
                                        Embed your form
                                    </LemonButton>
                                    <LemonButton type="tertiary" size="xsmall" icon={<IconQuestion />}>
                                        Help center
                                    </LemonButton>
                                </div>
                                <div className="flex flex-col gap-1">
                                    <span className="text-xs font-semibold uppercase tracking-wide text-muted mb-1">
                                        How-to guides
                                    </span>
                                    <LemonButton type="tertiary" size="xsmall" icon={<IconShuffle />}>
                                        Conditional logic
                                    </LemonButton>
                                    <LemonButton type="tertiary" size="xsmall" icon={<IconTarget />}>
                                        Targeting rules
                                    </LemonButton>
                                    <LemonButton type="tertiary" size="xsmall" icon={<IconPalette />}>
                                        Custom styling
                                    </LemonButton>
                                    <LemonButton type="tertiary" size="xsmall" icon={<IconBook />}>
                                        API &amp; webhooks
                                    </LemonButton>
                                </div>
                            </div>
                        </div>
                    )}
                </div>
            </div>
        </SceneContent>
    )
}
