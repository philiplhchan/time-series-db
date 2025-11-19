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
                    "service:logger"
                  ],
                  "boost" : 1.0
                }
              },
              {
                "terms" : {
                  "labels" : [
                    "name:logs"
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
                    "service:logger"
                  ],
                  "boost" : 1.0
                }
              },
              {
                "terms" : {
                  "labels" : [
                    "name:attempts"
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
                    "service:logger"
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
            "must_not" : [
              {
                "terms" : {
                  "labels" : [
                    "error_type:backoff"
                  ],
                  "boost" : 1.0
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
                    "service:logger"
                  ],
                  "boost" : 1.0
                }
              },
              {
                "wildcard" : {
                  "labels" : {
                    "wildcard" : "error_type:*",
                    "boost" : 1.0
                  }
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
                    "service:logger"
                  ],
                  "boost" : 1.0
                }
              },
              {
                "terms" : {
                  "labels" : [
                    "name:attempts"
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
                  "service:logger"
                ],
                "boost" : 1.0
              }
            },
            {
              "terms" : {
                "labels" : [
                  "name:logs"
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
                  "service:logger"
                ],
                "boost" : 1.0
              }
            },
            {
              "terms" : {
                "labels" : [
                  "name:attempts"
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
    "6" : {
      "filter" : {
        "bool" : {
          "filter" : [
            {
              "terms" : {
                "labels" : [
                  "service:logger"
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
          "must_not" : [
            {
              "terms" : {
                "labels" : [
                  "error_type:backoff"
                ],
                "boost" : 1.0
              }
            }
          ],
          "adjust_pure_negative" : true,
          "boost" : 1.0
        }
      },
      "aggregations" : {
        "6_unfold" : {
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
    "8" : {
      "filter" : {
        "bool" : {
          "filter" : [
            {
              "terms" : {
                "labels" : [
                  "service:logger"
                ],
                "boost" : 1.0
              }
            },
            {
              "wildcard" : {
                "labels" : {
                  "wildcard" : "error_type:*",
                  "boost" : 1.0
                }
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
        "8_unfold" : {
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
    "16" : {
      "filter" : {
        "bool" : {
          "filter" : [
            {
              "terms" : {
                "labels" : [
                  "service:logger"
                ],
                "boost" : 1.0
              }
            },
            {
              "terms" : {
                "labels" : [
                  "name:attempts"
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
        "16_unfold" : {
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
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "subtract",
            "right_op_reference" : "2",
            "keep_nans" : false
          },
          {
            "type" : "abs"
          }
        ],
        "references" : {
          "0" : "0>0_unfold",
          "2" : "2>2_unfold"
        },
        "inputReference" : "0"
      }
    },
    "10" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "divide",
            "right_op_reference" : "8",
            "labels" : [
              "error_type"
            ]
          },
          {
            "type" : "abs"
          },
          {
            "type" : "transform_null",
            "fill_value" : 0.0
          },
          {
            "type" : "moving",
            "interval" : 300000,
            "function" : "avg"
          }
        ],
        "references" : {
          "6" : "6>6_unfold",
          "8" : "8>8_unfold"
        },
        "inputReference" : "6"
      }
    },
    "14" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "union",
            "right_op_reference" : "10"
          },
          {
            "type" : "sum"
          }
        ],
        "references" : {
          "4" : "4",
          "10" : "10"
        },
        "inputReference" : "4"
      }
    },
    "18" : {
      "coordinator_pipeline" : {
        "buckets_path" : [ ],
        "stages" : [
          {
            "type" : "as_percent",
            "right_op_reference" : "16"
          },
          {
            "type" : "moving",
            "interval" : 300000,
            "function" : "min"
          },
          {
            "type" : "alias",
            "pattern" : "percent"
          },
          {
            "type" : "truncate",
            "min_timestamp" : 1000000000,
            "max_timestamp" : 1001000000
          }
        ],
        "references" : {
          "14" : "14",
          "16" : "16>16_unfold"
        },
        "inputReference" : "14"
      }
    }
  }
}
