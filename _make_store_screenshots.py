"""
把用戶傳的 5 張 iPhone 截圖處理成 App Store + Google Play 上架規格。

來源：C:/Users/sfy56/Downloads/S__148112{46,47,48,49,50}_0.jpg (1206x2622)

輸出：
  static/store-screenshots/ios/{1..5}.png  →  1290x2796 (App Store 6.7")
  static/store-screenshots/android/{1..5}.png  →  1080x1920 (Google Play)
"""
import os
from PIL import Image

SRC = 'C:/Users/sfy56/Downloads'
OUT_DIR = 'D:/python程式開發/飆股篩選器-OK/static/store-screenshots'
BG_COLOR = (10, 22, 40)  # #0a1628 午夜藍

# 5 張截圖 + 各自要裁掉的範圍（top, bottom，已知截圖原始尺寸 1206x2622）
SCREENS = [
    # (filename, top_crop, bottom_crop, label)
    ('S__14811246_0.jpg', 200, 0,   '篩選結果'),       # PWA 全螢幕
    ('S__14811247_0.jpg', 300, 200, '我的收藏'),       # Safari
    ('S__14811248_0.jpg', 200, 0,   'K線圖+公司資訊'), # PWA
    ('S__14811249_0.jpg', 300, 200, '首頁儀表板'),    # Safari
    ('S__14811250_0.jpg', 300, 200, '策略說明'),      # Safari
]

# 兩個 store 的目標尺寸（直式 portrait）
TARGETS = [
    ('ios',     (1290, 2796)),   # App Store 6.7" iPhone
    ('android', (1080, 1920)),   # Google Play
]


def crop_chrome(img, top, bottom):
    """裁掉頂部和底部的 browser chrome。"""
    w, h = img.size
    return img.crop((0, top, w, h - bottom))


def fit_to_canvas(content, target_size):
    """把內容圖等比例縮放後置中到目標尺寸畫布上，上下用深藍背景補。"""
    tw, th = target_size
    cw, ch = content.size

    # 等比例縮放，以 width 為主
    scale_w = tw / cw
    new_h = int(ch * scale_w)

    if new_h <= th:
        # 高度沒超出 → 縮放後上下加 padding
        scaled = content.resize((tw, new_h), Image.LANCZOS)
        canvas = Image.new('RGB', target_size, BG_COLOR)
        canvas.paste(scaled, (0, (th - new_h) // 2))
        return canvas
    else:
        # 高度超出 → 縮放後 crop center vertical
        scaled = content.resize((tw, new_h), Image.LANCZOS)
        offset = (new_h - th) // 2
        return scaled.crop((0, offset, tw, offset + th))


def main():
    for store, size in TARGETS:
        os.makedirs(os.path.join(OUT_DIR, store), exist_ok=True)

    for i, (fname, top, bottom, label) in enumerate(SCREENS, 1):
        src = os.path.join(SRC, fname)
        img = Image.open(src).convert('RGB')
        content = crop_chrome(img, top, bottom)
        print(f'{i}. {label}: {img.size} → crop → {content.size}')

        for store, size in TARGETS:
            out = os.path.join(OUT_DIR, store, f'{i}.png')
            final = fit_to_canvas(content, size)
            final.save(out, 'PNG', optimize=True)
            print(f'    {store}/{i}.png  →  {size}')

    print(f'\n完成！輸出在 {OUT_DIR}')


if __name__ == '__main__':
    main()
