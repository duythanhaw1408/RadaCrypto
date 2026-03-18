from __future__ import annotations

from cfte.execution.models import ExecutionSummary, FillFact, OrderSnapshot


def build_execution_summary(
    orders: list[OrderSnapshot],
    fills: list[FillFact],
    reconciliation,
    quality,
) -> ExecutionSummary:
    open_orders = [item for item in orders if item.status in {"NEW", "ACKED", "PARTIALLY_FILLED"}]
    return ExecutionSummary(
        total_orders=len(orders),
        open_orders=len(open_orders),
        closed_orders=len(orders) - len(open_orders),
        total_fills=len(fills),
        reconciliation=reconciliation,
        quality=quality,
    )


def render_execution_summary_vi(summary: ExecutionSummary) -> str:
    alignment = "khớp" if summary.reconciliation.is_aligned else "lệch"
    issue_status = "có" if summary.reconciliation.has_structural_issues else "không"
    return "\n".join(
        [
            "Tóm tắt giám sát thực thi:",
            f"- Tổng lệnh: {summary.total_orders} (mở: {summary.open_orders}, đóng: {summary.closed_orders})",
            f"- Tổng fill: {summary.total_fills}",
            (
                "- Đối soát vị thế: "
                f"{alignment} | nội bộ={summary.reconciliation.internal_net_qty:.6f}, "
                f"venue={summary.reconciliation.venue_net_qty:.6f}, "
                f"delta={summary.reconciliation.delta_qty:.6f}"
            ),
            (
                "- Kiểm tra đối soát mạnh hơn: "
                f"mua={summary.reconciliation.gross_buy_qty:.6f}, "
                f"bán={summary.reconciliation.gross_sell_qty:.6f}, "
                f"fill duy nhất={summary.reconciliation.unique_fill_count}, "
                f"trùng={summary.reconciliation.duplicate_fill_count}, "
                f"đến muộn/đảo thứ tự={summary.reconciliation.out_of_order_fill_count}, "
                f"vi phạm số lượng={summary.reconciliation.qty_violation_count}, "
                f"lỗi cấu trúc={issue_status}"
            ),
            (
                "- Chất lượng khớp lệnh: "
                f"slippage TB={summary.quality.avg_slippage_bps:.2f} bps, "
                f"markout TB={summary.quality.avg_markout_bps:.2f} bps"
            ),
        ]
    )
