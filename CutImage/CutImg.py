import os
from PIL import Image


def slice_image_horizontally(image_path, output_dir, output_prefix, slices=5):
    # 打开图片
    img = Image.open(image_path)
    width, height = img.size

    # 如果输出目录不存在，则创建该目录
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 计算每一片的高度（注意：如果图片高度不能被5整除，最后一片会包含剩余的像素）
    slice_height = height // slices

    for i in range(slices):
        # 定义每个切片的边界
        left = 0
        upper = i * slice_height
        # 如果是最后一片，则包含剩余的部分
        right = width
        lower = (i + 1) * slice_height if i < slices - 1 else height

        # 裁剪图片
        cropped_img = img.crop((left, upper, right, lower))
        # 构造输出文件完整路径
        output_path = os.path.join(output_dir, f"{output_prefix}_{i+1}.png")
        cropped_img.save(output_path)
        print(f"保存切片 {i+1} 到 {output_path}")


if __name__ == "__main__":
    # 替换为你的图片路径和输出前缀
    input_image_path = "D:\\OneDrive - Larch2352\\图片\\本机照片\\2025\\01\\IMG_20250122_171034.jpg"
    output_directory = "CutImage"
    output_prefix = "CutImage"
    slice_image_horizontally(input_image_path, output_directory, output_prefix, slices=5)
