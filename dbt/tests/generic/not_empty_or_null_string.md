{% docs test_not_empty_or_null_string %}

# WHAT

This test checks that the specified column does not contain any empty strings
or null values.

# WHY & WHEN

Ensuring that a column does not have empty strings or null values is crucial
for data integrity.

# HOW

```yaml
columns:
  - name: your_column_name
    tests:
      - not_empty_or_null_string
```

{% enddocs %}
