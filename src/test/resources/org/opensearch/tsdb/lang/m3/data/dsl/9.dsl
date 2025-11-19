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
                    "name:a"
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
                    "name:b"
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
                    "name:c"
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
                    "name:d"
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
                  "name:a"
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
        "0_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 1000000000,
            "max_timestamp" : 1001000000,
            "step" : 100000
          }
        }
      }
    },
    "1" : {
      "filter" : {
        "bool" : {
          "filter" : [
            {
              "terms" : {
                "labels" : [
                  "name:b"
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
        "1_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 1000000000,
            "max_timestamp" : 1001000000,
            "step" : 100000
          }
        }
      }
    },
    "3" : {
      "filter" : {
        "bool" : {
          "filter" : [
            {
              "terms" : {
                "labels" : [
                  "name:c"
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
        "3_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 1000000000,
            "max_timestamp" : 1001000000,
            "step" : 100000
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
                  "name:d"
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
        "4_unfold" : {
          "time_series_unfold" : {
            "min_timestamp" : 1000000000,
            "max_timestamp" : 1001000000,
            "step" : 100000
          }
        }
      }
    },
    "2" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "as_percent",
            "right_op_reference" : "1"
          }
        ],
        "references" : {
          "0" : "0>0_unfold",
          "1" : "1>1_unfold"
        },
        "inputReference" : "0"
      }
    },
    "5" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "as_percent",
            "right_op_reference" : "4"
          }
        ],
        "references" : {
          "3" : "3>3_unfold",
          "4" : "4>4_unfold"
        },
        "inputReference" : "3"
      }
    },
    "6" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "as_percent",
            "right_op_reference" : "5"
          },
          {
            "type" : "alias",
            "pattern" : "three word alias"
          }
        ],
        "references" : {
          "2" : "2",
          "5" : "5"
        },
        "inputReference" : "2"
      }
    }
  }
}
