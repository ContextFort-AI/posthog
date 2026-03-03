from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("posthog", "1025_drop_team_session_recording_encryption_column"),
    ]

    operations = [
        migrations.AddField(
            model_name="hogflow",
            name="draft",
            field=models.JSONField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="hogflow",
            name="draft_updated_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
