{
  "size" : 0,
  "query" : {
    "bool" : {
      "should" : [
        {
          "bool" : {
            "filter" : [
              {
                "terms" : {
                  "labels" : [
                    "country:us"
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
            "must_not" : [
              {
                "wildcard" : {
                  "labels" : {
                    "wildcard" : "region:north*",
                    "boost" : 1.0
                  }
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        },
        {
          "bool" : {
            "filter" : [
              {
                "terms" : {
                  "labels" : [
                    "state:xyz"
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
        {
          "bool" : {
            "filter" : [
              {
                "terms" : {
                  "labels" : [
                    "city:toronto"
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
        }
      ],
      "adjust_pure_negative" : true,
      "minimum_should_match" : "1",
      "boost" : 1.0
    }
  },
  "track_total_hits" : -1,
  "aggregations" : {
    "0" : {
      "filter" : {
        "bool" : {
          "filter" : [
            {
              "terms" : {
                "labels" : [
                  "country:us"
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
          "must_not" : [
            {
              "wildcard" : {
                "labels" : {
                  "wildcard" : "region:north*",
                  "boost" : 1.0
                }
              }
            }
          ],
          "adjust_pure_negative" : true,
          "boost" : 1.0
        }
      },
      "aggregations" : {
        "0_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 1000000000,
            "max_timestamp" : 1001000000,
            "step" : 100000,
            "stages" : [
              {
                "type" : "transform_null",
                "fill_value" : 0.0
              }
            ]
          }
        }
      }
    },
    "2" : {
      "filter" : {
        "bool" : {
          "filter" : [
            {
              "terms" : {
                "labels" : [
                  "state:xyz"
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
      "aggregations" : {
        "2_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 1000000000,
            "max_timestamp" : 1001000000,
            "step" : 100000
          }
        }
      }
    },
    "5" : {
      "filter" : {
        "bool" : {
          "filter" : [
            {
              "terms" : {
                "labels" : [
                  "city:toronto"
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
      "aggregations" : {
        "5_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 1000000000,
            "max_timestamp" : 1001000000,
            "step" : 100000
          }
        }
      }
    },
    "3" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "union",
            "right_op_reference" : "2"
          },
          {
            "type" : "sum"
          }
        ],
        "references" : {
          "0" : "0>0_unfold",
          "2" : "2>2_unfold"
        },
        "inputReference" : "0"
      }
    },
    "6" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "union",
            "right_op_reference" : "5"
          }
        ],
        "references" : {
          "3" : "3",
          "5" : "5>5_unfold"
        },
        "inputReference" : "3"
      }
    }
  }
}
