from tortoise import fields, models


class VariableChanges(models.Model):
    id = fields.IntField(pk=True)
    node_id = fields.CharField(max_length=200, null=False)
    variable_name = fields.CharField(max_length=200, null=False)
    timestamp = fields.DatetimeField(null=False)
    value = fields.FloatField()

    class Meta:
        table = "variable_changes"


class Events(models.Model):
    id = fields.IntField(pk=True)
    name = fields.CharField(max_length=200, null=False)
    severity = fields.IntField(null=False)
    message = fields.CharField(max_length=500, null=False)
    timestamp = fields.DatetimeField(null=False)
    source_node = fields.CharField(max_length=200, null=False)
