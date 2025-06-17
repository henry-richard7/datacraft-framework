# Usage for `CtlColumnMetadata`

## Column Json Mapping

#### **Description**

Defines the JSON path used to extract a list of values from nested JSON response from API.

---

#### **Example JSON Path**

#### **Sample JSON**

```json
{
  "analytics": {
    "sections": [
      {
        "items": [
          {
            "dailyStats": [
              { "date": "2024-06-01", "views": 100 },
              { "date": "2024-06-02", "views": 150 }
            ]
          },
          {
            "dailyStats": [{ "date": "2024-06-03", "views": 130 }]
          }
        ]
      },
      {
        "items": [
          {
            "dailyStats": [{ "date": "2024-06-04", "views": 170 }]
          }
        ]
      }
    ]
  }
}
```

#### Extracted Values Using JSON Path

```json
["2024-06-01", "2024-06-02", "2024-06-03", "2024-06-04"]
```

#### Explanation of Path

- analytics.sections[*]: Loops through each section in the analytics data.

- items[*]: Accesses each item within a section.

- dailyStats[*].date: Extracts the date field from each daily stat record.
