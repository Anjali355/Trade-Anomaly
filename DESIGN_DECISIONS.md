# DESIGN_DECISIONS.md

## 1. Planted Anomalies: Logic & Business Impact

We planted **12 anomalies** across the data to test the multi-layer detection system. These represent high-frequency, high-impact errors in Indian export operations.

| Anomaly Category | Count | Realistic Context | Cost of Non-Detection |
| --- | --- | --- | --- |
| **Financial (Layer 1)** | 4 | Unit price vs. Total FOB mismatches or excessive insurance. | Audit penalties from GST departments (India) for misdeclaration. |
| **Compliance (Layer 1)** | 3 | CIF shipments with $0 freight; invalid HS code lengths. | Customs hold-ups and demurrages at ports (₹15,000+/day). |
| **Logistics (Layer 2)** | 2 | Transit times or freight costs exceeding 1.5x IQR. | Unchecked logistics "leakage" and choosing inefficient freight forwarders. |
| **Classification (Layer 3)** | 2 | "Stainless Steel" products assigned "Textile" HS codes. | Wrongful duty claims; risk of "Blacklisting" by Customs (ICEGATE). |
| **Behavioral (Layer 2)** | 1 | Sudden payment delay (spike in `days_to_payment`). | Working capital crunch and increased cost of borrowing. |

---

## 2. Statistical Method: Why IQR?

For Layer 2, I chose the **Interquartile Range (IQR)** method over Z-scores or Isolation Forest.

* **Why not Z-Score?** Shipment data (pricing and transit times) is rarely "normally distributed." It is often skewed by high-value outliers or seasonal delays. Z-scores are sensitive to extreme outliers, which can "pull" the mean and lead to false negatives.
* **Why IQR?** It is based on medians and quartiles, making it robust against extreme values. It effectively ignores the "tails" of the data to find a true "normal" range.
* **Business Fit:** In trade finance, we care about values that fall outside the "middle 50%" of historical norms. IQR provides a clear, defensible boundary () for flagging anomalies to human auditors.

---

## 3. LLM Strategy: Layering and Usage

The system follows a "Filter-First" architecture.

* **The Line:** Layers 1 and 2 handle structured, numeric, and format-based data. Layer 3 (LLM) is reserved only for **Semantic Validation**—specifically where a text description must be compared against a numeric code (HS Code).
* **Data Sent:** We only sent 2 targeted fields to the LLM: `product_description` and `hs_code`. We did **not** send the entire 250-row CSV.
* **Usage Report Summary:**
* **Total Calls:** 2 (Only triggered for high-risk classification checks).
* **Token Efficiency:** By using a local cache (`self.response_cache`), we ensure that identical product/HS code pairs are never processed twice, reducing cost by 0% for recurring shipments.
* **Provider:** Groq (Llama-3-70b) for high-speed inference.



---

## 4. Prompt Iteration: "The Hallucination Fix"

**Initial (Bad) Prompt:**

> "Is this HS code 60022000 correct for Stainless Steel? Answer yes or no."

* **The Problem:** The LLM would often apologize or provide a long paragraph. Sometimes it would say "Yes" because it found a vague connection to "material," leading to false negatives.
* **The Improved Prompt:**

> "Act as a Trade Compliance Officer. Analyze the product: '{desc}' and HS Code: '{code}'.
> Return ONLY a JSON object with:
> { 'is_mismatch': boolean, 'reason': 'short string', 'confidence': float }.
> If the product material is fundamentally different from the HS chapter (e.g., Metal vs. Fabric), set is_mismatch to true."

**Result:** This forced a structured output that the Python backend could parse reliably without regex errors.

---

## 5. Performance: Precision and Recall

Based on `accuracy_report.json`:

**Analysis:**

* **Why the Miss?** One HS Code mismatch was subtle (within the same sub-chapter), and the LLM confidence threshold was set too high to flag it.
* **Why the False Positives?** These occurred in Layer 2 (Price Outliers). A "Spice Seasoning Mix" was flagged because its price was 18% lower than the median. In the real world, this might be a bulk discount, but the statistical engine saw it as an anomaly. To fix this, we would need to incorporate "Quantity-based pricing" into the statistical model.