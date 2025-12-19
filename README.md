大规模遥感栅格分层抽样优化器 (GeoSample Optimizer)

本项目提供了一套高效、可扩展的 Python 方案，用于从大规模 TIF 栅格影像（如 ESA WorldCover 10m 数据）中提取统计学公平的分层随机样点。

🚀 核心优势

针对处理 TB 级遥感数据时常见的 I/O 瓶颈和内存溢出问题，本项目采用了两阶段蓄水池采样 (Two-Stage Reservoir Sampling) 算法：

Map 阶段 (提取)：利用多进程并行扫描 TIF 文件，在内存中完成初步抽样，并将极其紧凑的中间结果保存为 .pkl 缓存。

Reduce 阶段 (合并)：通过流式读取中间缓存，瞬间完成全局样本合并，避免了传统方案中频繁读写海量矢量文件的巨大开销。

性能飞跃：在处理数千个栅格文件时，合并速度相比传统方案提升了 100倍以上。

🛠️ 技术栈

核心算法：分布式蓄水池采样 (Distributed Reservoir Sampling)

空间处理：rasterio, geopandas, shapely

高性能写入：pyogrio (基于 GDAL/OGR)

并发控制：multiprocessing 与进程间同步锁

📖 使用指南

1. 环境准备

确保您的 Python 环境中安装了必要的依赖项：

pip install -r requirements.txt


2. 第一阶段：特征提取

修改 script_stage1_extract.py 中的输入路径，运行：

python script_stage1_extract.py


该脚本将并行生成中间缓存文件，并实时打印各文件的地类分布诊断信息。

3. 第二阶段：最终抽样

运行 script_stage2_sample.py，根据提示输入目标地类 ID（如 10 代表林地），脚本将迅速生成最终的 .gpkg 矢量文件。

📊 性能表现

断点续传：基于 .done 标记文件，支持意外中断后自动恢复。

内存友好：无需一次性加载全量数据，内存占用恒定。

本项目适用于遥感深度学习样本库构建及土地利用分类精度验证。
