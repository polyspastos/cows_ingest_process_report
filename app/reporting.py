from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from datetime import datetime, timedelta
from app.models import Cow, MilkProduction, Weight


def generate_report(db_session: Session, report_date: datetime):
    report_data = {}

    milk_data = (
        db_session.query(
            MilkProduction.cow_id, func.sum(MilkProduction.value).label("total_milk")
        )
        .filter(func.date(MilkProduction.timestamp) == report_date.date())
        .group_by(MilkProduction.cow_id)
        .all()
    )

    for cow_id, total_milk in milk_data:
        report_data[cow_id] = {
            "total_milk": total_milk,
        }

    for cow_id, cow_name in db_session.query(Cow.id, Cow.name).all():
        latest_weight = (
            db_session.query(Weight.value)
            .filter(Weight.cow_id == cow_id)
            .order_by(Weight.timestamp.desc())
            .first()
        )

        thirty_days_ago = report_date - timedelta(days=30)
        avg_weight = (
            db_session.query(func.avg(Weight.value))
            .filter(and_(Weight.cow_id == cow_id, Weight.timestamp >= thirty_days_ago))
            .scalar()
        )

        report_data[cow_id].update(
            {
                "latest_weight": latest_weight[0] if latest_weight else None,
                "avg_weight_last_30_days": avg_weight,
            }
        )

    potential_illness = []
    for cow_id, data in report_data.items():
        if data.get("latest_weight") and data.get("avg_weight_last_30_days"):
            if data["latest_weight"] < (0.9 * data["avg_weight_last_30_days"]):
                potential_illness.append(cow_id)
        if data.get("total_milk") and data["total_milk"] < 5:
            potential_illness.append(cow_id)

    report = f"Report for {report_date.date()}\n"
    report += "=========================================\n"
    for cow_id, data in report_data.items():
        report += f"Cow ID: {cow_id}\n"
        report += f"Total Milk Production: {data.get('total_milk', 'N/A')} liters\n"
        report += f"Latest Weight: {data.get('latest_weight', 'N/A')} kg\n"
        report += (
            f"30-day Avg Weight: {data.get('avg_weight_last_30_days', 'N/A')} kg\n"
        )
        report += "-----------------------------------------\n"

    if potential_illness:
        report += "\nPotentially Ill Cows:\n"
        report += "\n".join(potential_illness)

    return report
