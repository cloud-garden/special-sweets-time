package posmining.nishimura.drink;

public class Sweets {
	private String receipt_id;
	private String jan_code;
	private int price;
	private int item_count;
	private String location;

	//以下は念のため作成

	public Sweets(String receipt_id, String jan_code, int price, int item_count, String location) {
		this.receipt_id = receipt_id;
		this.jan_code = jan_code;
		this.price = price;
		this.item_count = item_count;
		this.location = location;
	}

	//ドリンクをカウントするためのもの
	public Sweets(String receipt_id) {
		this.receipt_id = receipt_id;
	}

	public Sweets(){

	}

	public String getReceiptId() {
		return receipt_id;
	}

	public String getJanCode() {
		return jan_code;
	}

	public int getPrice() {
		return price;
	}

	public int getItemCount() {
		return item_count;
	}

	public String getLocation() {
		return location;
	}

	public void setReceiptId(String receipt_id) {
		this.receipt_id = receipt_id;
	}
	public void setJanCode(String jan_code) {
		this.jan_code = jan_code;
	}
	public void setPrice(int price) {
		this.price = price;
	}
	public void setItemCount(int item_count) {
		this.item_count = item_count;
	}
	public void setLocation(String location) {
		this.location = location;
	}
}