import os
import json
import requests
from typing import List, Optional
from cfte.tpfm.models import TPFM4hStructural

class TPFMAIExplainer:
    """Uses Gemini to explain TPFM 4h Structural reports in Vietnamese"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        self.model = "gemini-2.0-flash"
        self.url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent?key={self.api_key}"

    def explain_4h_structural(self, report: TPFM4hStructural) -> str:
        """Generates a Vietnamese market flow analysis for a 4h structural report"""
        if not self.api_key:
            return "⚠️ Thiếu GEMINI_API_KEY. Không thể tạo báo cáo AI."

        prompt = self._build_prompt(report)
        
        try:
            payload = {
                "contents": [{
                    "parts": [{"text": prompt}]
                }]
            }
            response = requests.post(self.url, json=payload, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            return data["candidates"][0]["content"]["parts"][0]["text"]
        except Exception as e:
            return f"❌ Lỗi khi gọi AI: {str(e)}"

    def _build_prompt(self, report: TPFM4hStructural) -> str:
        # Format metrics for the prompt
        regime_shares = ", ".join([f"{k}: {v*100:.0f}%" for k, v in report.dominant_regime_share.items()])
        path = " -> ".join(report.transition_map)
        
        prompt = f"""
Bạn là một chuyên gia phân tích dòng tiền (Order Flow) chuyên nghiệp. 
Hãy phân tích báo cáo cấu trúc thị trường 4H sau đây cho cặp tiền {report.symbol}:

DỮ LIỆU TPFM (Temporal Polarity Flow Matrix):
- Thời gian: {report.window_start_ts} đến {report.window_end_ts}
- Bias cấu trúc: {report.structural_bias}
- Tỉ lệ Regime: {regime_shares}
- Net Delta Quote (SPOT): {report.net_delta_quote:,.0f}
- Độ ổn định (Avg Persistence): {report.avg_persistence:.2f}
- Hành trình chuyển đổi (30m Path): {path}

BỐI CẢNH LIÊN THỊ TRƯỜNG (Context Overlay):
- Nếu có sự phân kỳ giữa Spot và Futures, hãy giải thích nguyên nhân (ví dụ: Futures Short-squeeze hay Spot Absorption).
- Đánh giá xem đà tăng/giảm là "Tự nhiên" (Spot dẫn dắt) hay "Đòn bẩy" (Futures dẫn dắt).

Yêu CẦU:
1. Viết 1-2 đoạn văn ngắn (tối đa 250 từ) bằng tiếng Việt.
2. Tập trung vào "Market Logic" và "Cross-Market Context".
3. Đưa ra nhận định về độ bền vững của cấu trúc hiện tại dựa trên cả dòng tiền Spot và bối cảnh Futures.
4. Không liệt kê lại các con số khô khan. 

PHONG CÁCH: Chuyên nghiệp, súc tích, thực chiến.
"""
        return prompt
