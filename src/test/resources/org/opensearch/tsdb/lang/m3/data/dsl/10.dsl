{
  "size" : 0,
  "query" : {
    "bool" : {
      "filter" : [
        {
          "terms" : {
            "labels" : [
              "service:web-api"
            ],
            "boost" : 1.0
          }
        },
        {
          "terms" : {
            "labels" : [
              "name:requests_total"
            ],
            "boost" : 1.0
          }
        },
        {
          "terms" : {
            "labels" : [
              "region:us-east",
              "region:us-west"
            ],
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
              "from" : 1000000000,
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
        "min_timestamp" : 1000000000,
        "max_timestamp" : 1001000000,
        "step" : 100000,
        "stages" : [
          {
            "type" : "per_second"
          },
          {
            "type" : "alias_by_tags",
            "tag_names" : [
              "service",
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
            "type" : "sort",
            "sortBy" : "avg",
            "sortOrder" : "desc"
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
