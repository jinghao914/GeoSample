import os
import glob
import random
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from tqdm import tqdm
import time
import pickle

# --- 1. 配置 (请修改这里) ---
TEMP_OUTPUT_DIR = r"F:\xiaochuang\ESA\linshi_optimized"  # 必须和 优化版脚本1 一致
# (新!) 最终输出目录 (我们将在这里创建多个gpkg)
FINAL_OUTPUT_DIR = r"F:\xiaochuang\ESA\all\by_class"
TARGET_SAMPLES = 2500  # 每个类别的目标样本数

# 地类定义
CLASS_MAP = {
    10: "林地", 20: "灌木", 30: "草地", 40: "耕地", 50: "建筑",
    60: "裸地/稀疏植被", 70: "冰/雪", 80: "永久水体",
    90: "湿地", 95: "红树林", 100: "苔藓、地衣"
}


# (不再需要 CLASSES_TO_SAMPLE_LIST)


# --- 2. (新) 用户输入目标地类 ---
def get_target_class():
    """
    在脚本开始时，向用户询问要处理哪个地类
    """
    print("--- 优化版脚本 2 (V-Opt-Single)：单地类提取 ---")
    print("\n可用的地类:")
    for cid, name in CLASS_MAP.items():
        print(f"  {cid}: {name}")

    target_cid_str = input(f"\n请输入您想 *本次* 提取的地类 ID (例如 10): ")
    try:
        target_cid = int(target_cid_str)
        if target_cid not in CLASS_MAP:
            raise ValueError
        target_name = CLASS_MAP[target_cid]
        print(f"--- 好的，本次将只提取: {target_cid} ({target_name}) ---")
        return target_cid, target_name
    except ValueError:
        print(f"!! 错误: '{target_cid_str}' 不是一个有效的地类 ID。退出。")
        return None, None


# --- 3. 主函数 (V-Opt-Single - 阶段2：合并采样) ---
def reduce_and_sample_phase2(target_cid, target_name):
    start_time = time.time()

    # (新!) 动态设置最终输出文件名
    os.makedirs(FINAL_OUTPUT_DIR, exist_ok=True)
    final_output_gpkg = os.path.join(FINAL_OUTPUT_DIR, f"FINAL_SAMPLES_CLASS_{target_cid}.gpkg")
    print(f"将保存到: {final_output_gpkg}")

    # 1. 查找 优化版脚本1 生成的 'temp_*.pkl' 文件
    temp_files = glob.glob(os.path.join(TEMP_OUTPUT_DIR, "temp_*.pkl"))
    temp_files = [f for f in temp_files if not f.endswith('.done')]  # 确保不抓取 .done

    if not temp_files:
        print(f"错误：在 {TEMP_OUTPUT_DIR} 中未找到任何 'temp_*.pkl' 文件。")
        print("请确保 优化版脚本 1 (optimized_script_1_extract_v2.py) 已成功运行。")
        return

    print(f"找到了 {len(temp_files)} 个临时 .pkl 文件，开始为地类 {target_cid} 合并...")

    # --- (核心优化) ---
    # (新!) 只为目标地类初始化蓄水池和计数器
    final_samples = []  # 最终的样本池 (阶段2)
    final_pass_count = 0  # 阶段2 采样的 'k'
    global_pixel_count = 0  # 全局真实像素总数 (用于报告)
    global_crs = None

    try:
        # *顺序* 读取每个 .pkl 文件 (非常快)
        for pkl_file in tqdm(temp_files, desc=f"合并地类 {target_cid} (阶段2)"):

            with open(pkl_file, 'rb') as f:
                # 加载 阶段1 的结果
                local_samples, local_counts, file_crs = pickle.load(f)

            if global_crs is None and file_crs is not None:
                global_crs = file_crs

            # 1. 累加全局 *像素* 计数 (只关心目标)
            global_pixel_count += local_counts.get(target_cid, 0)

            # 2. (核心) 将 阶段1 的样本 流式传入 阶段2 的蓄水池
            # (只获取目标地类的样本列表)
            points_list = local_samples.get(target_cid, [])

            for point_data in points_list:
                # point_data 是 (x, y, cid)

                # --- 执行 阶段2 蓄水池抽样 ---
                final_pass_count += 1
                k = final_pass_count

                if len(final_samples) < TARGET_SAMPLES:
                    final_samples.append(point_data)
                else:
                    j = random.randint(0, k - 1)
                    if j < TARGET_SAMPLES:
                        final_samples[j] = point_data
                # --- (阶段2 采样结束) ---

    except Exception as e:
        print(f"\n!! 致命错误: 处理 {pkl_file} 时出错: {e}")
        return

    print("\n--- 阶段 2 合并采样完成，正在准备保存结果 ---")

    # --- 3. 汇总和保存结果 ---

    num_found = len(final_samples)
    total_pixels = global_pixel_count

    print(f"地类 {target_cid} ({target_name}):")
    print(f"  > 共找到 {total_pixels} 个像素。")
    print(f"  > 成功抽取 {num_found} / {TARGET_SAMPLES} 个样本。")

    if total_pixels == 0:
        print(f"!! 警告: 在所有 .pkl 文件中未找到地类 {target_cid} 的任何像素。")
        print("--- 脚本结束 ---")
        return

    if total_pixels > 0 and total_pixels < TARGET_SAMPLES:
        print(f"  > 警告：该地类的总像素数 ({total_pixels}) 少于目标样本数，已抽取所有像素。")
    elif total_pixels > 0 and final_pass_count < TARGET_SAMPLES:
        print(f"  > 警告：所有文件的总样本数 ({final_pass_count}) 少于目标样本数，已抽取所有样本。")

    print(f"\n总共采集到 {num_found} 个样本点。")

    # (来自旧脚本2的相同保存逻辑)
    df = pd.DataFrame(final_samples, columns=['x', 'y', 'class_id'])
    # (新) 手动添加 class_name，因为我们知道它是什么
    df['class_name'] = target_name
    geometry = [Point(xy) for xy in zip(df['x'], df['y'])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=global_crs)

    # --- 4. 保存 ---
    try:
        # (推荐) 尝试使用 pyogrio 引擎
        gdf.to_file(final_output_gpkg, driver="GPKG", engine="pyogrio", layer=f"samples_class_{target_cid}")
        end_time = time.time()
        print(f"\n--- 成功！(使用 pyogrio 引擎写入) ---")
    except Exception:
        try:
            print("\n(pyogrio 失败, 尝试使用 fiona 引擎...)")
            gdf.to_file(final_output_gpkg, driver="GPKG", layer=f"samples_class_{target_cid}")
            end_time = time.time()
            print(f"\n--- 成功！(使用 fiona 引擎写入) ---")
        except Exception as e_fiona:
            print(f"\n!! 错误：保存最终文件失败: {e_fiona}")
            return

    print(f"本次运行 ({target_name}) 耗时: {(end_time - start_time):.2f} 秒")
    print(f"最终样本点已保存到: {final_output_gpkg}")


# --- 5. 启动 ---
if __name__ == "__main__":
    # 1. 先获取用户想跑哪个
    target_class_id, target_class_name = get_target_class()

    # 2. 如果用户输入有效，才执行处理
    if target_class_id is not None:
        reduce_and_sample_phase2(target_class_id, target_class_name)