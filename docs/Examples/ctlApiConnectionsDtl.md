# Usage for `ctlApiConnectionsDtl`

#### **Description**

The `body_values` column should contain a list of key-value mappings where each key is a placeholder (wrapped in `$<...>$`) and the value is a list of items that will replace that placeholder.

---

### 📌 Format

```json
[
  {
    "$<placeholder1>$": ["value1", "value2"]
  },
  {
    "$<placeholder2>$": ["valueA", "valueB"]
  }
]
```

---

### 📋 Example

```json
[
  {
    "$campaign_id$": ["CMP001", "CMP002"]
  },
  {
    "$region$": ["US", "EU"]
  }
]
```

This format is typically used for dynamically constructing API request bodies where placeholders need to be replaced with actual data during execution.

---

> 💡 Placeholders must be wrapped in `$...$`. Values must be provided as arrays, even for single replacements.
