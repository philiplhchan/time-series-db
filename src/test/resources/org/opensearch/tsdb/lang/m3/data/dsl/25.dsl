{
  "size" : 0,
  "query" : {
    "time_range_pruner" : {
      "min_timestamp" : 1000000000,
      "max_timestamp" : 1001000000,
      "query" : {
        "bool" : {
          "filter" : [
            {
              "range" : {
                "timestamp_range" : {
                  "from" : 1000000000,
                  "to" : 1001000000,
                  "include_lower" : true,
                  "include_upper" : false,
                  "boost" : 1.0
                }
              }
            },
            {
              "terms" : {
                "labels" : [
                  "name:service.requests"
                ],
                "boost" : 1.0
              }
            },
            {
              "cached_wildcard" : {
                "wildcard" : {
                  "labels" : {
                    "wildcard" : "env:prod-*",
                    "boost" : 1.0
                  }
                }
              }
            }
          ],
          "adjust_pure_negative" : true,
          "boost" : 1.0
        }
      },
      "boost" : 1.0
    }
  },
  "track_total_hits" : -1,
  "aggregations" : {
    "0_unfold" : {
      "time_series_unfold" : {
        "min_timestamp" : 1000000000,
        "max_timestamp" : 1001000000,
        "step" : 100000,
        "stages" : [
          {
            "type" : "tag_sub",
            "tag_name" : "env",
            "search_pattern" : "^prod-(.*)$",
            "replacement" : "production-$1"
          },
          {
            "type" : "alias",
            "pattern" : "{{.env}}"
          }
        ]
      }
    }
  }
}
