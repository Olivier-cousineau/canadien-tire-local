const FORMAT_SEGMENTS = [3, 4, 1];

const extractDigits = (value) => {
  if (value == null) return null;
  const match = String(value).match(/(\d{3})\D*(\d{4})\D*(\d)\b/);
  if (match) {
    return `${match[1]}${match[2]}${match[3]}`;
  }
  const rawDigits = String(value).match(/\b(\d{8})\b/);
  return rawDigits ? rawDigits[1] : null;
};

const formatProductNumber = (digits) => {
  if (!digits || digits.length !== 8) return null;
  const [a, b, c] = FORMAT_SEGMENTS;
  return `${digits.slice(0, a)}-${digits.slice(a, a + b)}-${digits.slice(a + b, a + b + c)}`;
};

export const normalizeCtProductNumber = (value) => {
  const digits = extractDigits(value);
  return digits ? formatProductNumber(digits) : null;
};

export const makeCtProductKey = (value) => {
  const normalized = normalizeCtProductNumber(value);
  return normalized ? `ct:${normalized}` : null;
};

export const buildCtKeysFromText = (value) => {
  const digits = extractDigits(value);
  if (!digits) {
    return { productNumberRaw: null, productNumber: null, productKey: null };
  }
  const productNumber = formatProductNumber(digits);
  return {
    productNumberRaw: digits,
    productNumber,
    productKey: productNumber ? `ct:${productNumber}` : null,
  };
};
