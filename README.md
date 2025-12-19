Large-Scale Geospatial Raster Sampling Optimizer

(大规模遥感栅格数据分层抽样优化器)

这是一个针对大规模 TIF 栅格数据（如 ESA WorldCover 10m）进行高效分层随机抽样的工具。

🚀 项目背景与痛点 (Problem Statement)

在地理信息系统（GIS）和遥感图像处理中，从数千个高分辨率栅格文件中提取均匀分布的训练样本是一个巨大的挑战。原始的串行或简单并行方案通常会遇到以下瓶颈：

I/O 灾难：尝试并发写入数千个小型 GeoPackage 文件，并在后续合并时同时打开这些文件，导致极其严重的磁盘随机读写，处理速度呈指数级下降。

内存压力：传统的全量加载方法在面对 TB 级数据时会导致内存溢出（OOM）。

统计偏差：简单的局部采样难以保证全局范围内不同地类的比例公平性。

✨ 核心解决方案：Map-Reduce 采样架构

本项目借鉴了分布式计算中的 Map-Reduce 思想，引入了两阶段蓄水池采样 (Two-Stage Reservoir Sampling) 算法，实现了从 $O(N^2)$ 到 $O(N)$ 的性能飞跃。

阶段 1：Map - 并行提取与局部采样 (script_stage1_extract.py)

多进程驱动：充分利用多核 CPU，每个进程独立扫描栅格文件。

内存采样：在读取 .tif 的过程中直接进行蓄水池采样，仅保留每个文件内部最具代表性的样本点。

二进制中间层：将局部样本及元数据保存为极小的 .pkl 文件，避免了创建大量矢量文件的磁盘开销。

实时诊断：实时反馈每个文件的地类分布情况，便于监控数据异常。

阶段 2：Reduce - 全局合并与精准抽样 (script_stage2_sample.py)

流式合并：顺序读取第一阶段产生的中间文件，消除了磁盘寻道延迟。

全局抽样：在所有样本中进行第二次公平抽样，严格确保最终样点数量符合 TARGET_SAMPLES 设定。

空间优化：最终结果输出为 GeoPackage 格式，并可选配 pyogrio 引擎以获得更快的写入速度。

📊 性能表现对比

处理阶段

原始方案 (V1-V4)

优化方案 (Map-Reduce)

提升幅度

Stage 1 (提取)

慢（受限于磁盘写入）

快（受限于 CPU 与读取）

~3x

Stage 2 (合并)

极慢（受限于随机 I/O）

极快（顺序读取）

100x+

总耗时

几小时至几天

几分钟至几小时

数量级提升

🛠️ 安装与使用

环境要求

pip install rasterio geopandas pandas shapely tqdm pyogrio


快速开始

配置：打开 script_stage1_extract.py，修改 RASTER_DIR 指向你的栅格文件夹。

运行提取：执行 python script_stage1_extract.py。

生成样本：执行 python script_stage2_sample.py（可根据提示选择提取特定地类或全部合并）。

🛡️ 算法原理

$$[TIF_1, TIF_2, ..., TIF_n] \xrightarrow{Parallel\ Map} [PKL_1, PKL_2, ..., PKL_n] \xrightarrow{Stream\ Reduce} Final\ Samples$$

许可证

本项目采用 MIT License 开源协议。
