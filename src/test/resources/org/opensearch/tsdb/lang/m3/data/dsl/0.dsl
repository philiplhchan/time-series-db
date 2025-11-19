{
  "size" : 0,
  "query" : {
    "bool" : {
      "filter" : [
        {
          "terms" : {
            "labels" : [
              "name:service.errors"
            ],
            "boost" : 1.0
          }
        },
        {
          "bool" : {
            "should" : [
              {
                "terms" : {
                  "labels" : [
                    "region:ca"
                  ],
                  "boost" : 1.0
                }
              },
              {
                "wildcard" : {
                  "labels" : {
                    "wildcard" : "region:us-*",
                    "boost" : 1.0
                  }
                }
              }
            ],
            "adjust_pure_negative" : true,
            "minimum_should_match" : "1",
            "boost" : 1.0
          }
        },
        {
          "range" : {
            "min_timestamp" : {
              "from" : null,
              "to" : 1001000000,
              "include_lower" : true,
              "include_upper" : false,
              "boost" : 1.0
            }
          }
        },
        {
          "range" : {
            "max_timestamp" : {
              "from" : 999940000,
              "to" : null,
              "include_lower" : true,
              "include_upper" : true,
              "boost" : 1.0
            }
          }
        }
      ],
      "adjust_pure_negative" : true,
      "boost" : 1.0
    }
  },
  "track_total_hits" : -1,
  "aggregations" : {
    "0_unfold" : {
      "time_series_unfold" : {
        "min_timestamp" : 999940000,
        "max_timestamp" : 1001000000,
        "step" : 100000,
        "stages" : [
          {
            "type" : "transform_null",
            "fill_value" : 0.0
          },
          {
            "type" : "moving",
            "interval" : 60000,
            "function" : "sum"
          },
          {
            "type" : "sum",
            "group_by_labels" : [
              "region"
            ]
          }
        ]
      }
    },
    "0_coordinator" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "avg"
          },
          {
            "type" : "min",
            "group_by_labels" : [
              "region"
            ]
          },
          {
            "type" : "max"
          },
          {
            "type" : "keep_last_value"
          },
          {
            "type" : "value_filter",
            "operator" : "GE",
            "target_value" : 5.0
          },
          {
            "type" : "count"
          },
          {
            "type" : "alias",
            "pattern" : "{{.region}}"
          },
          {
            "type" : "truncate",
            "min_timestamp" : 1000000000,
            "max_timestamp" : 1001000000
          }
        ],
        "references" : {
          "0_unfold" : "0_unfold"
        },
        "inputReference" : "0_unfold"
      }
    }
  }
}
