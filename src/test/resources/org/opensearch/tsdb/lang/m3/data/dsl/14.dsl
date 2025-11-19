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
                    "name:metric1"
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
                    "from" : 999700000,
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
                    "name:metric2"
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
                    "from" : 999700000,
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
                    "name:metric3"
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
                    "from" : 999700000,
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
                    "name:metric4"
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
                    "from" : 999700000,
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
                  "name:metric1"
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
                  "from" : 999700000,
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
        "0_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 999700000,
            "max_timestamp" : 1001000000,
            "step" : 100000,
            "stages" : [
              {
                "type" : "sum"
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
                  "name:metric2"
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
                  "from" : 999700000,
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
            "min_timestamp" : 999700000,
            "max_timestamp" : 1001000000,
            "step" : 100000,
            "stages" : [
              {
                "type" : "sum"
              }
            ]
          }
        }
      }
    },
    "4" : {
      "filter" : {
        "bool" : {
          "filter" : [
            {
              "terms" : {
                "labels" : [
                  "name:metric3"
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
                  "from" : 999700000,
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
        "4_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 999700000,
            "max_timestamp" : 1001000000,
            "step" : 100000
          }
        }
      }
    },
    "7" : {
      "filter" : {
        "bool" : {
          "filter" : [
            {
              "terms" : {
                "labels" : [
                  "name:metric4"
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
                  "from" : 999700000,
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
        "7_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 999700000,
            "max_timestamp" : 1001000000,
            "step" : 100000,
            "stages" : [
              {
                "type" : "sum"
              }
            ]
          }
        }
      }
    },
    "5" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "union",
            "right_op_reference" : "4"
          },
          {
            "type" : "avg"
          }
        ],
        "references" : {
          "2" : "2>2_unfold",
          "4" : "4>4_unfold"
        },
        "inputReference" : "2"
      }
    },
    "7_coordinator" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "alias",
            "pattern" : "metric)() with parens"
          }
        ],
        "references" : {
          "7_unfold" : "7>7_unfold"
        },
        "inputReference" : "7_unfold"
      }
    },
    "10" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "union",
            "right_op_reference" : "7"
          },
          {
            "type" : "avg"
          }
        ],
        "references" : {
          "5" : "5",
          "7" : "7_coordinator"
        },
        "inputReference" : "5"
      }
    },
    "12" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "as_percent",
            "right_op_reference" : "10"
          },
          {
            "type" : "moving",
            "interval" : 300000,
            "function" : "min"
          },
          {
            "type" : "truncate",
            "min_timestamp" : 1000000000,
            "max_timestamp" : 1001000000
          }
        ],
        "references" : {
          "0" : "0>0_unfold",
          "10" : "10"
        },
        "inputReference" : "0"
      }
    }
  }
}
