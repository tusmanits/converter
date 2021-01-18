{% macro log_custom_event(event_name) %}
    {{ logging.log_audit_event(
        event_name, schema=this.schema, relation=this.name, user=target.user, target_name=target.name, is_full_refresh=flags.FULL_REFRESH
    ) }}
{% endmacro %}