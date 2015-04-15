package CouplesFrequency;

/**
 * Questa classe modella una coppia di valori (String, Integer) rappresentanti
 * la coppia di prodotti e l'effettiva quantita' venduta
 *
 */
public class CoupleProductOccurrence {
	public String product;
	public int occurrence;

	public CoupleProductOccurrence(String product, int occurrence) {
		this.product = product;
		this.occurrence = occurrence;
	}

	public CoupleProductOccurrence(String description) {
		String[] parts = description.split(":");
		this.product = parts[0];
		this.occurrence = Integer.parseInt(parts[1]);
	}

	@Override
	public String toString() {
		return this.product + ":" + this.occurrence;
	}

	public String getProduct() {
		return product;
	}

	public int getOccurrence() {
		return occurrence;
	}
};