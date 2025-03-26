import pandas as pd
import os
from src.outreach_generation.message_generator import MessageGenerator

# 创建测试数据
stakeholders_data = {
    'name': ['John Doe', 'Jane Smith'],
    'title': ['CEO', 'CTO'],
    'company': ['Company A', 'Company B'],
    'email': ['john@companya.com', 'jane@companyb.com'],
    'decision_making_power': ['High', 'Medium'],
    'relevance_score': ['High relevance for protective films', 'Medium relevance for coatings']
}

companies_data = {
    'name': ['Company A', 'Company B'],
    'description': ['Manufactures solar panels', 'Produces electronic devices'],
    'products': [['Solar Panels', 'Inverters'], ['Smartphones', 'Tablets']],
    'materials': [['Glass', 'Silicon'], ['Plastic', 'Metal']]
}

# 创建DataFrame
stakeholders_df = pd.DataFrame(stakeholders_data)
companies_df = pd.DataFrame(companies_data)

# 初始化消息生成器
message_generator = MessageGenerator()
# 强制使用模板消息
message_generator.openai_api_key = ""

# 测试消息生成
try:
    result_df = message_generator.generate_messages_for_stakeholders(stakeholders_df, companies_df)
    print("消息生成成功！")
    print(f"生成的消息数量: {len(result_df)}")
    print("\n第一条消息示例:")
    print(result_df['outreach_message'].iloc[0])
except Exception as e:
    print(f"测试失败，错误信息: {str(e)}")