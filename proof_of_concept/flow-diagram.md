run_full_experiment.sh

├─ setup_databases()
│   └─ Para cada par (SF, DB_NAME):
│       └─ SCALE_FACTOR=$sf DB_NAME=$db run_setup.sh
│           ├─ setup_database()
│           ├─ setup_tpch_tools()
│           ├─ generate_and_load_data()
│           └─ generate_queries()
├─ generate_experimental_design.py
  ├─ generate_treatment_combinations()
  │   └─ Crea todas las combinaciones:
  │       ├─ I/O Methods: sync, bgworkers, iouring
  │       ├─ DB Sizes: 0.1, 1, 10
  │       └─ Replicates: 12
  │       └─ Total: 3 × 3 × 12 = 108 runs
  ├─ generate_crd_schedule()
  │   └─ randomiza todas las combinaciones (CRD)

run_randomized_experiment.sh
└─ execute_experiment()
    └─ Para cada fila del CSV:
        ├─ execute_run(run_order, db_size_gb, io_method, replicate, db_name, treatment_id, cooldown)
        │   ├─ switch_database($db_name)
        │   ├─ configure_io_method($io_method)
        │   ├─ restart_postgresql()
        │   ├─ clear_caches()
        │   ├─ COOLDOWN PERIOD
        │   └─ EJECUCIÓN DEL BENCHMARK
        │           ├─ POWER TEST (execute_power_test)
        │           └─ THROUGHPUT TEST (execute_throughput_test)
        │       ├─ RECOLECCIÓN DE RESULTADOS
        │       ├─ CÁLCULO DE MÉTRICAS (calculate_qphh)