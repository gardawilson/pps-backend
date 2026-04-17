const QRCode = require('qrcode');
const { getBrowser } = require('./browser');

/**
 * Generate PDF label dari data + template function
 * @param {object} data         - data label, akan di-pass ke templateFn
 * @param {Function} templateFn - function(data) => HTML string
 * @param {object} options
 * @param {string} options.width  - lebar halaman (default: '80mm')
 * @returns {Buffer} PDF buffer
 */
async function generateLabelPdf(data, templateFn, options = {}) {
  const width = options.width || '80mm';

  // 1. Generate QR code sebagai base64
  const qrValue = data.noLabel || data.kode || 'NO-CODE';
  const qrBase64 = await QRCode.toDataURL(qrValue, {
    width: 200,
    margin: 1,
    errorCorrectionLevel: 'M',
  });

  // 2. Render HTML via template function
  const html = templateFn({ ...data, qrBase64 });

  // 3. Launch browser (reuse jika sudah ada)
  const browser = await getBrowser();
  const page = await browser.newPage();

  try {
    // Set viewport lebar dulu, tinggi sementara
    await page.setViewport({ width: 302, height: 800 });
    await page.setContent(html, { waitUntil: 'networkidle0' });

    // Baca tinggi konten aktual setelah render
    const contentHeight = await page.evaluate(
      () => document.body.scrollHeight
    );

    // Set viewport ulang sesuai tinggi konten agar tidak ada blank space
    await page.setViewport({ width: 302, height: contentHeight });

    const pdf = await page.pdf({
      width,
      height: `${contentHeight}px`,
      printBackground: true,
      margin: { top: 0, right: 0, bottom: 0, left: 0 },
    });

    return pdf;
  } finally {
    await page.close();
  }
}

module.exports = { generateLabelPdf };
