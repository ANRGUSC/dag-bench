# DAGBench Source Bibliography

Master list of all sources used to build the DAGBench workflow collection.

> **Provenance policy**: Every workflow has verified provenance. Papers cited below were actually read and verified. DOIs have been checked. Where workflows are AI-generated, the citation explains the relationship honestly. See each workflow's `metadata.yaml` for full provenance details including a `notes` field.

## Frameworks & Datasets

| Source | Type | Workflows | License |
|--------|------|-----------|---------|
| [SAGA](https://github.com/anrgusc/saga) (anrg-saga) | Framework | RIoTBench (4), random generators (6) | Non-commercial (USC/ANRG) |

## Papers Cited

All DOIs below have been verified. Papers marked with "(no DAG)" do not contain extractable task dependency graphs — the corresponding workflows are AI-generated structures inspired by the paper's domain.

### Scheduling Algorithms

- **HEFT**: Topcuoglu, H., Hariri, S., Wu, M.-Y. (2002). "Performance-effective and low-complexity task scheduling for heterogeneous computing." *IEEE TPDS*, 13(3). DOI: [10.1109/71.993206](https://doi.org/10.1109/71.993206)
- **PISA/SAGA**: Boggs, J., Sights, L., Krishnamachari, B. (2024). "PISA: A Comprehensive Benchmark for Scheduling Algorithms." arXiv: [2403.07120](https://arxiv.org/abs/2403.07120)

### IoT / Sensor Networks

- **RIoTBench**: Shukla, A., Chaturvedi, S., Simmhan, Y. (2017). "RIoTBench: An IoT Benchmark for Distributed Stream Processing Systems." DOI: [10.1109/SC2.2017.42](https://doi.org/10.1109/SC2.2017.42)

### Edge Computing — Paper-Extracted DAGs (manual-figure)

These papers contain explicit DAG figures from which workflows were faithfully extracted:

- **Wu et al. 2024** (Figure 1 → `edge.ml_surveillance_pipeline`): Wu, L., Hanafy, W.A., Souza, A., Abdelzaher, T., Verma, G., Shenoy, P. (2024). "Enhancing Resilience in Distributed ML Inference Pipelines for Edge Computing." *IEEE MILCOM*. DOI: [10.1109/MILCOM61039.2024.10773652](https://doi.org/10.1109/MILCOM61039.2024.10773652)
- **Liang et al. 2024** (Figure 1, Table 1 → `edge.splitstream_pipeline`): Liang, Y., Zhang, S., Wu, J. (2024). "SplitStream: Distributed and workload-adaptive video analytics at the edge." *J. Network and Computer Applications*. DOI: [10.1016/j.jnca.2024.103866](https://doi.org/10.1016/j.jnca.2024.103866)
- **Alghareb & Hasan 2025** (Figures 4-5 → `edge.face_analysis_pipeline`): "Pipeline-Parallel Face Analysis System for Edge Computing." *Computers (MDPI)*. DOI: [10.3390/computers14010029](https://doi.org/10.3390/computers14010029)
- **Romero et al. 2024** (Figure 2(a) → `edge.loki_traffic_pipeline`): Romero, K.K.W. et al. (2024). "Loki: A System for Serving ML Inference Pipelines with Hardware-Aware Scheduling." *HPDC*. DOI: [10.1145/3625549.3658688](https://doi.org/10.1145/3625549.3658688)
- **Li et al. 2024** (Figure 5 → `edge.mtec_lightgbm`, `edge.mtec_video_analytics`, `edge.mtec_matrix_ops`): Li, X., Abdallah, M., Lou, Y.-Y., Chiang, M., Kim, K.T., Bagchi, S. (2024). "Dynamic DAG-Application Scheduling for Multi-Tier Edge Computing in Heterogeneous Networks." arXiv: [2409.10839](https://arxiv.org/abs/2409.10839)

### MEC / Mobile Offloading — Code-Extracted DAGs (manual-figure)

These DAGs were faithfully extracted from open-source simulator source code:

- **De Maio & Brandic 2018** (Source code → `mec.sleipnir_navigator`, `mec.sleipnir_antivirus`, `mec.sleipnir_facerecognizer`, `mec.sleipnir_facebook`, `mec.sleipnir_chess`): De Maio, V., Brandic, I. (2018). "First Hop Mobile Offloading of DAG Computations." *IEEE/ACM CCGrid*. DOI: [10.1109/CCGRID.2018.00023](https://doi.org/10.1109/CCGRID.2018.00023). Source code: [github.com/vindem/sleipnir](https://github.com/vindem/sleipnir) (MIT license).

### Agriculture IoT — Paper-Extracted DAGs (manual-figure)

- **Ghafouri et al. 2025** (Figure 1 → `agri.flockfocus_feeder`): Ghafouri, S., Ding, Y., Diaz Chito, K., Martinez del Rincon, J., O'Connell, N., Vandierendonck, H. (2025). "Optimizing Video Analytics Inference Pipelines: A Case Study." arXiv: [2512.07009](https://arxiv.org/abs/2512.07009)

### Edge Computing — Other References

- **Guo et al. 2025** (no DAG match): Guo, K., Wang, H., Chen, G. (2025). "Dependent Task Offloading Model for Mobile Edge Computing." *MDPI Electronics*. DOI: [10.3390/electronics14163184](https://doi.org/10.3390/electronics14163184)
- **Liu et al. 2022** (no DAG match): Liu, B., Wang, Y., Wang, G., Cai, Z. (2022). "Collaborative Computation and Dependency-Aware Task Offloading for Vehicular Edge Computing." *J. Cloud Computing*. DOI: [10.1186/s13677-022-00340-3](https://doi.org/10.1186/s13677-022-00340-3)

### ML Pipelines

- **Nalinipriya et al. 2024** (no DAG match): Nalinipriya, G. et al. (2024). "Optimal directed acyclic graph federated learning model for energy-efficient IoT communication networks." *Scientific Reports*. DOI: [10.1038/s41598-024-71995-y](https://doi.org/10.1038/s41598-024-71995-y)

### Telecom / 5G

- **Adoga & Pezaros 2022** (no DAG): Adoga, H.I., Pezaros, D.P. (2022). "Network Function Virtualization and Service Function Chaining Frameworks: A Comprehensive Review." *Future Internet*. DOI: [10.3390/fi14020059](https://doi.org/10.3390/fi14020059)

### Scientific Workflows

- **Pegasus**: Deelman, E., Vahi, K., Juve, G. et al. (2015). "Pegasus, a workflow management system for science automation." *Future Generation Computer Systems*. DOI: [10.1016/j.future.2014.10.008](https://doi.org/10.1016/j.future.2014.10.008)
- **BLAST**: Altschul, S.F., Gish, W., Miller, W., Myers, E.W., Lipman, D.J. (1990). "Basic local alignment search tool." *J. Molecular Biology*. DOI: [10.1016/S0022-2836(05)80360-2](https://doi.org/10.1016/S0022-2836(05)80360-2)

### MEC / Task Offloading

- **Chen et al. 2016** (no DAG): Chen, X. et al. (2016). "Efficient multi-user computation offloading for mobile-edge cloud computing." *IEEE/ACM TNET*. DOI: [10.1109/TNET.2015.2487344](https://doi.org/10.1109/TNET.2015.2487344)
- **Mach & Becvar 2017** (no DAG): Mach, P., Becvar, Z. (2017). "Mobile edge computing: A survey on architecture and computation offloading." *IEEE COMST*. DOI: [10.1109/COMST.2017.2682318](https://doi.org/10.1109/COMST.2017.2682318)
- **Ananthanarayanan et al. 2017** (no DAG): Ananthanarayanan, G. et al. (2017). "Real-time video analytics: The killer app for edge computing." *IEEE Computer*. DOI: [10.1109/MC.2017.3641638](https://doi.org/10.1109/MC.2017.3641638)
- **You et al. 2017** (no DAG): You, C., Huang, K., Chae, H. (2017). "Energy efficient mobile cloud computing powered by wireless energy transfer." *IEEE TWC*. DOI: [10.1109/TWC.2016.2633522](https://doi.org/10.1109/TWC.2016.2633522)

### V2X / Vehicular

- **Chen et al. 2019** (no DAG): Chen, Q. et al. (2019). "Cooper: Cooperative perception for connected autonomous vehicles." *IEEE ICDCS*. DOI: [10.1109/ICDCS.2019.00058](https://doi.org/10.1109/ICDCS.2019.00058)
- **Dresner & Stone 2008** (no DAG): Dresner, K., Stone, P. (2008). "A multiagent approach to autonomous intersection management." *JAIR*. DOI: [10.1613/jair.2502](https://doi.org/10.1613/jair.2502)
- **Neumeier et al. 2019** (no DAG): Neumeier, S. et al. (2019). "Teleoperation: The holy grail to solve problems of automated driving?" *TMA Workshop*. DOI: [10.23919/TMA.2019.8784466](https://doi.org/10.23919/TMA.2019.8784466)

### UAV / Drone

- **Mogili & Deepak 2018** (no DAG): Mogili, U.R., Deepak, B.B.V.L. (2018). "Review on application of drone systems in precision agriculture." *Procedia CS*. DOI: [10.1016/j.procs.2018.07.063](https://doi.org/10.1016/j.procs.2018.07.063)
- **Zeng et al. 2016** (no DAG): Zeng, Y., Zhang, R., Lim, T.J. (2016). "Wireless communications with unmanned aerial vehicles: Opportunities and challenges." *IEEE COMST*. DOI: [10.1109/COMST.2016.2592520](https://doi.org/10.1109/COMST.2016.2592520)
- **Hayat et al. 2016** (no DAG): Hayat, S., Yanmaz, E., Muzaffar, R. (2016). "Survey on unmanned aerial vehicle networks for civil applications." *IEEE COMST*. DOI: [10.1109/COMST.2016.2560343](https://doi.org/10.1109/COMST.2016.2560343)

### Smart City

- **Djahel et al. 2015** (no DAG): Djahel, S., Doolan, R., Muntean, G.-M., Murphy, J. (2015). "A communications-oriented perspective on traffic management systems for smart cities." *IEEE COMST*. DOI: [10.1109/COMST.2014.2339817](https://doi.org/10.1109/COMST.2014.2339817)
- **Morawska et al. 2018** (no DAG): Morawska, L. et al. (2018). "Applications of low-cost sensing technologies for air quality monitoring and exposure assessment." *Environment International*. DOI: [10.1016/j.envint.2018.04.018](https://doi.org/10.1016/j.envint.2018.04.018)

### Industrial IoT

- **Teti et al. 2010** (no DAG): Teti, R., Jemielniak, K., O'Donnell, G., Dornfeld, D. (2010). "Advanced monitoring of machining operations." *CIRP Annals*. DOI: [10.1016/j.cirp.2010.05.010](https://doi.org/10.1016/j.cirp.2010.05.010)
- **Tao et al. 2019** (no DAG): Tao, F., Zhang, H., Liu, A., Nee, A.Y.C. (2019). "Digital Twin in Industry: State-of-the-Art." *IEEE TII*. DOI: [10.1109/TII.2018.2873186](https://doi.org/10.1109/TII.2018.2873186)
- **Xu et al. 2014** (no DAG): Xu, L.D., He, W., Li, S. (2014). "Internet of Things in Industries: A Survey." *IEEE TII*. DOI: [10.1109/TII.2014.2300753](https://doi.org/10.1109/TII.2014.2300753)
- **Wan et al. 2018** (no DAG): Wan, J. et al. (2018). "Reconfigurable Smart Factory for Drug Packing in Healthcare Industry 4.0." *IEEE TII*. DOI: [10.1109/TII.2018.2843811](https://doi.org/10.1109/TII.2018.2843811)

### Agriculture IoT

- **Ojha et al. 2015** (no DAG): Ojha, T., Misra, S., Raghuwanshi, N.S. (2015). "Wireless sensor networks for agriculture: The state-of-the-art in practice and future challenges." *Computers & Electronics in Agriculture*. DOI: [10.1016/j.compag.2015.08.011](https://doi.org/10.1016/j.compag.2015.08.011)
- **Mohanty et al. 2016** (no DAG): Mohanty, S.P., Hughes, D.P., Salathe, M. (2016). "Using Deep Learning for Image-Based Plant Disease Detection." *Frontiers in Plant Science*. DOI: [10.3389/fpls.2016.01419](https://doi.org/10.3389/fpls.2016.01419)

### Fog Computing

- **Bonomi et al. 2014** (no DAG): Bonomi, F., Milito, R., Natarajan, P., Zhu, J. (2014). "Fog Computing: A Platform for Internet of Things and Analytics." *Springer*. DOI: [10.1007/978-3-319-05029-4_7](https://doi.org/10.1007/978-3-319-05029-4_7)
- **Yousefpour et al. 2019** (no DAG): Yousefpour, A. et al. (2019). "All One Needs to Know about Fog Computing and Related Edge Computing Paradigms." *J. Systems Architecture*. DOI: [10.1016/j.sysarc.2019.02.009](https://doi.org/10.1016/j.sysarc.2019.02.009)
- **Negash et al. 2018** (no DAG): Negash, B. et al. (2018). "Leveraging Fog Computing for Healthcare IoT." *Springer*. DOI: [10.1007/978-3-319-57639-8_8](https://doi.org/10.1007/978-3-319-57639-8_8)

### NFV / Telecom

- **Foukas et al. 2017** (no DAG): Foukas, X. et al. (2017). "Network Slicing in 5G: Survey and Challenges." *IEEE Comms. Mag.* DOI: [10.1109/MCOM.2017.1600951](https://doi.org/10.1109/MCOM.2017.1600951)

### Classic Benchmarks

- **MapReduce**: Dean, J., Ghemawat, S. (2004). "MapReduce: Simplified Data Processing on Large Clusters." DOI: [10.1145/1327452.1327492](https://doi.org/10.1145/1327452.1327492)
- **Cholesky/LU**: Buttari, A., Langou, J., Kurzak, J., Dongarra, J. (2009). "Scheduling of QR factorization algorithms on SMP and multi-core architectures." DOI: [10.1016/j.parco.2008.12.008](https://doi.org/10.1016/j.parco.2008.12.008)
