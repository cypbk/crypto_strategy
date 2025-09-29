#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
加密貨幣多策略交易系統主程式
改造後的加密貨幣版本入口點
更新日期: 2025-01-27
"""

import sys
import os
from datetime import datetime

# 添加src目錄到Python路徑
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.crypto_screener import CryptoMultiStrategyScreener
from src.utils.logger import setup_logger

# 建立日誌器
logger = setup_logger(__name__)


def display_menu():
    """顯示主選單"""
    print("="*80)
    print("🚀 加密貨幣多策略交易系統 (CryptoMultiStrategyScreener)")
    print("="*80)
    print("請選擇執行模式：")
    print("1. 完整執行（資料更新 + 所有策略信號）")
    print("2. 僅更新資料庫")
    print("3. 僅產生海龜信號")
    print("4. 僅產生BNF信號")
    print("5. 僅產生蓄勢待發信號")
    print("6. 海龜 + BNF信號（不更新資料）")
    print("7. 自訂策略組合")
    print("8. 檢查資料庫狀態")
    print("9. 查看策略資訊")
    print("10. 系統測試")
    print("11. 更新交易對清單")
    print("0. 退出程式")
    print("="*80)


def get_user_choice():
    """獲取用戶選擇"""
    while True:
        try:
            choice = input("請輸入選項 (0-11): ").strip()
            if choice in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11']:
                return choice
            else:
                print("❌ 無效選項，請重新輸入")
        except KeyboardInterrupt:
            print("\n👋 程式已退出")
            sys.exit(0)


def get_custom_strategies():
    """獲取自訂策略組合"""
    print("\n可用的策略:")
    print("1. turtle (海龜策略)")
    print("2. bnf (BNF策略)")
    print("3. coiled_spring (蓄勢待發策略)")
    
    strategies = []
    while True:
        try:
            choice = input("請輸入策略編號 (多個用逗號分隔，直接按Enter結束): ").strip()
            if not choice:
                break
            
            strategy_map = {'1': 'turtle', '2': 'bnf', '3': 'coiled_spring'}
            selected = [strategy_map[c.strip()] for c in choice.split(',') if c.strip() in strategy_map]
            strategies.extend(selected)
            
            if selected:
                print(f"已選擇策略: {', '.join(selected)}")
            else:
                print("❌ 無效的策略編號")
                
        except KeyboardInterrupt:
            break
    
    return list(set(strategies))  # 去重


def get_additional_options():
    """獲取額外選項"""
    options = {}
    
    # 詢問是否要強制更新交易對清單
    while True:
        try:
            update_pairs = input("是否強制更新交易對清單? (y/n, 預設n): ").strip().lower()
            if update_pairs in ['y', 'yes', 'n', 'no', '']:
                options['force_update_pairs'] = update_pairs in ['y', 'yes']
                break
            else:
                print("❌ 請輸入 y 或 n")
        except KeyboardInterrupt:
            break
    
    return options


def run_full_execution(screener):
    """執行完整分析"""
    logger.info("🚀 開始完整執行...")
    
    options = get_additional_options()
    
    # 如果需要，先更新交易對清單
    if options.get('force_update_pairs', False):
        logger.info("🔄 更新交易對清單...")
        pairs = screener.adapter.update_pairs_list(force_update=True)
        if pairs:
            logger.info(f"✅ 成功更新 {len(pairs)} 個交易對")
        else:
            logger.warning("⚠️ 交易對更新失敗")
    
    signals = screener.run_screening(force_update=True)
    
    if signals:
        total_signals = sum(len(s) for s in signals.values())
        logger.info(f"✅ 完整執行完成，共產生 {total_signals} 個信號")
    else:
        logger.warning("⚠️ 未產生任何信號")


def run_database_update(screener):
    """僅更新資料庫"""
    logger.info("🔄 開始資料庫更新...")
    
    success = screener.update_database_only()
    
    if success:
        logger.info("✅ 資料庫更新完成")
    else:
        logger.error("❌ 資料庫更新失敗")


def run_single_strategy(screener, strategy_name):
    """執行單一策略"""
    logger.info(f"🔍 開始執行 {strategy_name} 策略...")
    
    # 提供資料更新選項
    print("\n資料更新選項：")
    print("a. 自動判斷是否需要更新")
    print("b. 強制更新資料")
    print("c. 跳過更新（使用現有資料）")
    
    while True:
        update_choice = input("請選擇 (a/b/c): ").strip().lower()
        
        if update_choice in ['a', 'b', 'c']:
            # 設定更新參數
            force_update = (update_choice == 'b')
            skip_update = (update_choice == 'c')
            
            signals = screener.run_screening([strategy_name], 
                                           force_update=force_update, 
                                           skip_update=skip_update)
            
            if signals and strategy_name in signals:
                signal_count = len(signals[strategy_name])
                logger.info(f"✅ {strategy_name} 策略完成，產生 {signal_count} 個信號")
            else:
                logger.warning(f"⚠️ {strategy_name} 策略未產生任何信號")
            break
        else:
            print("❌ 無效選項，請輸入 a、b 或 c")


def run_multiple_strategies(screener, strategy_names):
    """執行多個策略"""
    logger.info(f"🔍 開始執行策略組合: {', '.join(strategy_names)}")
    
    # 提供資料更新選項
    print("\n資料更新選項：")
    print("a. 自動判斷是否需要更新")
    print("b. 強制更新資料")
    print("c. 跳過更新（使用現有資料）")
    
    while True:
        update_choice = input("請選擇 (a/b/c): ").strip().lower()
        
        if update_choice in ['a', 'b', 'c']:
            # 設定更新參數
            force_update = (update_choice == 'b')
            skip_update = (update_choice == 'c')
            
            signals = screener.run_screening(strategy_names,
                                           force_update=force_update,
                                           skip_update=skip_update)
            
            if signals:
                total_signals = sum(len(s) for s in signals.values())
                logger.info(f"✅ 策略組合執行完成，共產生 {total_signals} 個信號")
            else:
                logger.warning("⚠️ 策略組合未產生任何信號")
            break
        else:
            print("❌ 無效選項，請輸入 a、b 或 c")


def check_database_status(screener):
    """檢查資料庫狀態"""
    logger.info("🔍 檢查資料庫狀態...")
    
    status = screener.get_database_status()
    
    print("\n" + "="*50)
    print("📊 加密貨幣資料庫狀態報告")
    print("="*50)
    print(f"連接狀態: {'✅ 正常' if status.get('is_connected') else '❌ 異常'}")
    print(f"記錄總數: {status.get('total_records', 0):,}")
    print(f"交易對數量: {status.get('total_symbols', 0)}")
    print(f"資料新鮮度: {status.get('data_freshness', 'unknown')}")
    print(f"最新日期: {status.get('latest_date', '無')}")
    print(f"資料庫大小: {status.get('db_size_mb', 0)} MB")
    print(f"日期範圍: {status.get('date_range', '無')}")
    
    if not status.get('is_connected'):
        print(f"錯誤: {status.get('error', '未知錯誤')}")
    
    print("="*50)


def show_strategy_info(screener):
    """顯示策略資訊"""
    logger.info("📋 獲取策略資訊...")
    
    strategy_info = screener.get_strategy_info()
    
    print("\n" + "="*60)
    print("📋 加密貨幣策略資訊")
    print("="*60)
    
    for strategy_name, info in strategy_info.items():
        print(f"\n🔹 {strategy_name.upper()} 策略:")
        print(f"   名稱: {info['name']}")
        print(f"   描述: {info['description'][:100]}...")
        print(f"   配置參數: {len(info['config'])} 個")
        
        # 顯示關鍵參數
        config = info['config']
        if 'min_price' in config:
            print(f"   最低價格: {config['min_price']} USDT")
        if 'min_volume' in config:
            print(f"   最低成交量: {config['min_volume']:,} USDT")
    
    print("="*60)


def run_system_test(screener):
    """執行系統測試"""
    logger.info("🔍 開始系統測試...")
    
    test_results = screener.test_system()
    
    print("\n" + "="*50)
    print("🔍 系統測試結果")
    print("="*50)
    
    all_passed = True
    for component, status in test_results.items():
        status_text = "✅ 正常" if status else "❌ 異常"
        print(f"{component}: {status_text}")
        if not status:
            all_passed = False
    
    print("="*50)
    
    if all_passed:
        print("🎉 所有系統組件測試通過！")
    else:
        print("⚠️ 部分系統組件測試失敗，請檢查配置")


def update_pairs_list(screener):
    """更新交易對清單"""
    logger.info("🔄 更新交易對清單...")
    
    print("\n更新選項：")
    print("a. 檢查現有清單（不強制更新）")
    print("b. 強制更新清單")
    
    while True:
        choice = input("請選擇 (a/b): ").strip().lower()
        
        if choice in ['a', 'b']:
            force_update = (choice == 'b')
            
            pairs = screener.adapter.update_pairs_list(force_update=force_update)
            
            if pairs:
                print(f"\n✅ 成功獲取 {len(pairs)} 個交易對")
                print("\n前10個交易對:")
                for i, pair in enumerate(pairs[:10], 1):
                    print(f"  {i}. {pair}")
                
                if len(pairs) > 10:
                    print(f"  ... 還有 {len(pairs) - 10} 個交易對")
            else:
                print("❌ 無法獲取交易對清單")
            break
        else:
            print("❌ 無效選項，請輸入 a 或 b")


def main():
    """主程式"""
    try:
        # 初始化篩選器
        logger.info("🚀 初始化加密貨幣多策略篩選器...")
        screener = CryptoMultiStrategyScreener()
        logger.info("✅ 篩選器初始化完成")
        
        while True:
            display_menu()
            choice = get_user_choice()
            
            if choice == '0':
                print("👋 程式執行完畢")
                break
            elif choice == '1':
                run_full_execution(screener)
            elif choice == '2':
                run_database_update(screener)
            elif choice == '3':
                run_single_strategy(screener, 'turtle')
            elif choice == '4':
                run_single_strategy(screener, 'bnf')
            elif choice == '5':
                run_single_strategy(screener, 'coiled_spring')
            elif choice == '6':
                run_multiple_strategies(screener, ['turtle', 'bnf'])
            elif choice == '7':
                custom_strategies = get_custom_strategies()
                if custom_strategies:
                    run_multiple_strategies(screener, custom_strategies)
                else:
                    print("❌ 未選擇任何策略")
            elif choice == '8':
                check_database_status(screener)
            elif choice == '9':
                show_strategy_info(screener)
            elif choice == '10':
                run_system_test(screener)
            elif choice == '11':
                update_pairs_list(screener)
            
            # 等待用戶確認繼續
            if choice != '0':
                input("\n按Enter鍵繼續...")
                print("\n")
    
    except KeyboardInterrupt:
        print("\n👋 程式已退出")
    except Exception as e:
        logger.error(f"❌ 程式執行錯誤: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        logger.info("程式結束")


if __name__ == "__main__":
    main()
