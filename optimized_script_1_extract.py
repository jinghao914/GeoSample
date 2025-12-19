import os
import glob
import rasterio
import numpy as np
from tqdm import tqdm
import multiprocessing
import time
import pickle  # 使用 pickle/joblib 进行高效的临时对象存储
import random

# --- 1. 配置 (您必须修改这里) ---
RASTER_DIR = r"F:\xiaochuang\ESA"
FILE_PATTERN = "*.tif"
TEMP_OUTPUT_DIR = r"F:\xiaochuang\ESA\linshi_optimized"  # <-- 建议使用新的临时目录
N_CORES_TO_USE = 3
TARGET_SAMPLES = 20000  # 蓄水池大小，从脚本2移到这里

# --- 2. 地类定义 ---
CLASS_MAP = {
    10: "林地", 20: "灌木", 30: "草地", 40: "耕地", 50: "建筑",
    60: "裸地/稀疏植被", 70: "冰/雪", 80: "永久水体",
    90: "湿地", 95: "红树林", 100: "苔藓、地衣"
}
CLASSES_TO_SAMPLE_LIST = list(CLASS_MAP.keys())


# --- 3. 工作函数 (V-Opt: 处理 *单个* 文件并进行 *阶段1* 采样) ---
def process_file_and_sample(raster_file, config):
    """
    由单个CPU核心调用:
    1. 读取 *一个* 栅格文件
    2. 在内存中对该文件中的点进行 *阶段1* 蓄水池采样
    3. 将采样结果 (一个小的样本池) 和 *总计数* 保存到 .pkl 文件
    """
    TEMP_OUTPUT_DIR = config['temp_dir']
    CLASSES_TO_SAMPLE_LIST = config['classes_list']
    TARGET_SAMPLES = config['target_samples']

    # (新!) 输出文件名基于输入文件名
    base_name = os.path.basename(raster_file)
    output_filename = os.path.join(TEMP_OUTPUT_DIR, f"temp_{base_name}.pkl")
    # (新!) “完成标记”文件名
    done_filename = os.path.join(TEMP_OUTPUT_DIR, f"temp_{base_name}.pkl.done")

    # (!! 关键修复 !!) ---
    # 1. 检查是否已完成 (如果 .done 文件存在)
    if os.path.exists(done_filename):
        return (raster_file, "skipped")

    # --- (核心优化) ---
    # 在内存中为 *这一个文件* 初始化蓄水池和计数器
    # 这现在是 *阶段 1* 的采样
    local_samples = {class_id: [] for class_id in CLASSES_TO_SAMPLE_LIST}
    local_counts = {class_id: 0 for class_id in CLASSES_TO_SAMPLE_LIST}
    file_crs = None
    total_points_found = 0

    try:
        with rasterio.open(raster_file) as src:
            file_crs = src.crs
            if file_crs is None:
                raise ValueError(f"文件 {raster_file} 没有CRS信息!")

            transform = src.transform  # 获取 transform

            # 逐块读取
            for ji, window in src.block_windows(1):
                block = src.read(1, window=window)

                # (微优化: 先检查块中是否包含任何目标类)
                if not np.any(np.isin(block, CLASSES_TO_SAMPLE_LIST)):
                    continue

                mask = np.isin(block, CLASSES_TO_SAMPLE_LIST)
                rows, cols = np.where(mask)
                if rows.size == 0:
                    continue

                class_ids_flat = block[rows, cols]
                global_rows = rows + window.row_off
                global_cols = cols + window.col_off

                # (微优化: 仅在需要时计算坐标)
                xs, ys = rasterio.transform.xy(transform, global_rows, global_cols)

                # --- (核心优化) 在内存中执行蓄水池抽样 ---
                for x, y, cid_raw in zip(xs, ys, class_ids_flat):
                    cid = int(cid_raw)
                    # (跳过不在列表中的类, 尽管isin已经过滤了)
                    if cid not in local_counts:
                        continue

                    local_counts[cid] += 1
                    k = local_counts[cid]
                    point_data = (x, y, cid)  # 存储为 (x, y, class_id) 元组
                    total_points_found += 1

                    if len(local_samples[cid]) < TARGET_SAMPLES:
                        local_samples[cid].append(point_data)
                    else:
                        j = random.randint(0, k - 1)
                        if j < TARGET_SAMPLES:
                            local_samples[cid][j] = point_data
                # --- (内存采样结束) ---

        # 2. (核心优化) 保存 *阶段1* 的结果
        # 我们保存两个关键信息：
        #   1. local_samples: 包含最多 N*TARGET_SAMPLES 个点的字典
        #   2. local_counts: 包含该文件中每个地类 *总像素数* 的字典
        #   3. file_crs: 文件的坐标系
        result_payload = (local_samples, local_counts, file_crs)

        with open(output_filename, 'wb') as f_pkl:
            pickle.dump(result_payload, f_pkl)

        # 3. (成功!) 创建“完成标记”文件
        with open(done_filename, 'w') as f_done:
            f_done.write(f"completed on {time.ctime()}")

        return (raster_file, total_points_found)

    except Exception as e:
        print(f"!! 致命错误 [进程] 处理 {raster_file} 失败: {e}")
        # 如果失败，删除可能已损坏的pkl
        if os.path.exists(output_filename):
            os.remove(output_filename)
        return (raster_file, "failed")


# --- 4. 主函数 (“分发”任务) ---
def main():
    print("--- 优化版脚本 1 (V-Opt)：并行提取 & 阶段1采样 ---")
    print(f"将动用 {N_CORES_TO_USE} 个CPU核心。")
    print("此版本将样本池保存为高效的 .pkl 文件，而不是完整的 .gpkg。")

    if not os.path.exists(TEMP_OUTPUT_DIR):
        os.makedirs(TEMP_OUTPUT_DIR)

    # 1. 查找所有目标栅格文件
    all_raster_files = glob.glob(os.path.join(RASTER_DIR, FILE_PATTERN))
    if not all_raster_files:
        print(f"!!! 致命错误：在 {RASTER_DIR} 中未找到任何 {FILE_PATTERN} 文件。")
        return

    print(f"总共找到了 {len(all_raster_files)} 个栅格文件。")

    # 2. 检查哪些文件 *尚未* 完成
    files_to_process = []
    for f in all_raster_files:
        base_name = os.path.basename(f)
        done_filename = os.path.join(TEMP_OUTPUT_DIR, f"temp_{base_name}.pkl.done")
        if not os.path.exists(done_filename):
            files_to_process.append(f)

    if not files_to_process:
        print("\n--- 所有文件均已处理完毕！---")
        print(f"您可以直接运行 优化版脚本 2 (optimized_script_2_reduce.py) 了。")
        return

    print(f"--- 状态：{len(all_raster_files) - len(files_to_process)} 个已完成，{len(files_to_process)} 个待处理。 ---")
    time.sleep(3)  # 让用户看到

    # 3. 准备任务
    config = {
        'temp_dir': TEMP_OUTPUT_DIR,
        'classes_list': CLASSES_TO_SAMPLE_LIST,
        'target_samples': TARGET_SAMPLES
    }
    tasks = [(f, config) for f in files_to_process]

    print(f"开始处理 {len(tasks)} 个剩余文件...")

    start_time = time.time()
    with multiprocessing.Pool(processes=N_CORES_TO_USE) as pool:
        results = list(tqdm(pool.imap_unordered(process_single_file_wrapper, tasks),
                            total=len(tasks),
                            desc="处理栅格文件"))

    end_time = time.time()
    print("\n--- 本次运行完成! ---")
    print(f"耗时: {(end_time - start_time) / 60:.2f} 分钟")

    # 统计结果
    processed = [r for r in results if r[1] != "skipped" and r[1] != "failed"]
    skipped = [r for r in results if r[1] == "skipped"]
    failed = [r for r in results if r[1] == "failed"]

    print(f"本次运行: {len(processed)} 个文件被处理, {len(failed)} 个失败, {len(skipped)} 个被跳过。")

    # 再次检查
    done_files = glob.glob(os.path.join(TEMP_OUTPUT_DIR, "*.pkl.done"))
    if len(done_files) == len(all_raster_files):
        print("\n*** 所有文件均已处理完毕！***")
        print("*** 现在请运行 优化版脚本 2 (optimized_script_2_reduce.py) 来合并最终结果。***")
    else:
        print(f"\n*** 尚未完成。总进度: {len(done_files)} / {len(all_raster_files)}。***")
        print("*** 您可以随时重新运行此脚本来继续。***")


def process_single_file_wrapper(args):
    # 辅助函数，用于解包 imap_unordered 的参数
    return process_file_and_sample(*args)


if __name__ == "__main__":
    multiprocessing.set_start_method('spawn')
    main()