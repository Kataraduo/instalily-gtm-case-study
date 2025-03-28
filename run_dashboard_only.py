"""
仅生成仪表盘的简化脚本 - 使用已有的公司数据
"""
import logging
import pandas as pd
from pathlib import Path
from src.data_enrichment.company_enricher import CompanyEnricher
from src.data_enrichment.stakeholder_finder import StakeholderFinder
from src.outreach_generation.message_generator import MessageGenerator
from src.visualization.dashboard_generator import DashboardGenerator
from src.config.config import OUTPUT_DATA_DIR

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """使用已有的公司数据生成仪表盘的主函数"""
    logger.info("开始使用已有的公司数据生成仪表盘")
    
    # 创建输出目录
    output_dir = Path(OUTPUT_DATA_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 加载已有的公司数据
    companies_df = pd.read_csv("data/processed/test_companies.csv")
    logger.info(f"已加载 {len(companies_df)} 家公司的数据")
    
    # 步骤1: 丰富公司数据
    logger.info("步骤1: 丰富公司数据")
    company_enricher = CompanyEnricher()
    enriched_companies_df = company_enricher.enrich_companies_data(companies_df)
    enriched_companies_df.to_csv(output_dir / "enriched_companies.csv", index=False)
    logger.info(f"已丰富 {len(enriched_companies_df)} 家公司的数据")
    
    # 步骤2: 查找利益相关者
    logger.info("步骤2: 查找利益相关者")
    stakeholder_finder = StakeholderFinder()
    stakeholders_df = stakeholder_finder.find_stakeholders(enriched_companies_df)
    stakeholders_df.to_csv(output_dir / "stakeholders.csv", index=False)
    logger.info(f"已找到 {len(stakeholders_df)} 个利益相关者")
    
    # 步骤3: 生成外联消息
    logger.info("步骤3: 生成外联消息")
    message_generator = MessageGenerator()
    stakeholders_with_messages_df = message_generator.generate_messages(stakeholders_df, enriched_companies_df)
    stakeholders_with_messages_df.to_csv(output_dir / "stakeholders_with_messages.csv", index=False)
    logger.info(f"已为 {len(stakeholders_with_messages_df)} 个利益相关者生成外联消息")
    
    # 步骤4: 生成仪表盘
    logger.info("步骤4: 生成仪表盘")
    dashboard_generator = DashboardGenerator()
    dashboard_path = dashboard_generator.generate_dashboard(stakeholders_with_messages_df, enriched_companies_df)
    logger.info(f"仪表盘已生成: {dashboard_path}")
    
    logger.info("仪表盘生成流程已完成")
    logger.info(f"所有输出文件保存在: {output_dir}")
    logger.info(f"仪表盘路径: {dashboard_path}")
    
    # 返回仪表盘路径，以便可以在浏览器中打开
    return dashboard_path

if __name__ == "__main__":
    dashboard_path = main()
    print(f"\n仪表盘已生成，请在浏览器中打开以下文件查看:\n{dashboard_path}")