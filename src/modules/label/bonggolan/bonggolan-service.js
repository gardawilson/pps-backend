// services/bonggolan-service.js
const { sql, poolPromise } = require('../../../core/config/db');

exports.getAll = async ({ page, limit, search }) => {
  const pool = await poolPromise;
  const request = pool.request();
  const offset = (page - 1) * limit;

  const baseQuery = `
    SELECT
      b.NoBonggolan,
      b.DateCreate,
      b.IdBonggolan,
      mb.NamaBonggolan,                -- 🆕 join ke master
      b.IdWarehouse,
      w.NamaWarehouse,
      b.Blok,
      b.IdLokasi,
      b.Berat,
      CASE 
        WHEN b.IdStatus = 1 THEN 'PASS'
        WHEN b.IdStatus = 0 THEN 'HOLD'
        ELSE ''
      END AS StatusText,

      -- Broker Output
      MAX(bpo.NoProduksi)    AS BrokerNoProduksi,
      MAX(m1.NamaMesin)      AS BrokerNamaMesin,

      -- Inject Output
      MAX(ipo.NoProduksi)    AS InjectNoProduksi,
      MAX(m2.NamaMesin)      AS InjectNamaMesin,

      -- Bongkar Susun
      MAX(bs.NoBongkarSusun) AS NoBongkarSusun

    FROM [dbo].[Bonggolan] b
    LEFT JOIN [dbo].[MstBonggolan] mb
           ON mb.IdBonggolan = b.IdBonggolan          -- 🆕
    LEFT JOIN [dbo].[MstWarehouse] w
           ON w.IdWarehouse = b.IdWarehouse

    -- Broker chain
    LEFT JOIN [dbo].[BrokerProduksiOutputBonggolan] bpo
           ON bpo.NoBonggolan = b.NoBonggolan
    LEFT JOIN [dbo].[BrokerProduksi_h] bp
           ON bp.NoProduksi = bpo.NoProduksi
    LEFT JOIN [dbo].[MstMesin] m1
           ON m1.IdMesin = bp.IdMesin

    -- Inject chain
    LEFT JOIN [dbo].[InjectProduksiOutputBonggolan] ipo
           ON ipo.NoBonggolan = b.NoBonggolan
    LEFT JOIN [dbo].[InjectProduksi_h] ip
           ON ip.NoProduksi = ipo.NoProduksi
    LEFT JOIN [dbo].[MstMesin] m2
           ON m2.IdMesin = ip.IdMesin

    -- Bongkar Susun
    LEFT JOIN [dbo].[BongkarSusunOutputBonggolan] bs
           ON bs.NoBonggolan = b.NoBonggolan

    WHERE 1=1
      AND b.DateUsage IS NULL
      ${
        search
          ? `AND (
               b.NoBonggolan LIKE @search
               OR b.Blok LIKE @search
               OR CONVERT(VARCHAR(20), b.IdLokasi) LIKE @search
               OR CONVERT(VARCHAR(20), b.IdWarehouse) LIKE @search
               OR ISNULL(w.NamaWarehouse,'') LIKE @search
               OR ISNULL(mb.NamaBonggolan,'') LIKE @search     -- 🆕 searchable

               OR ISNULL(bpo.NoProduksi,'') LIKE @search
               OR ISNULL(m1.NamaMesin,'') LIKE @search
               OR ISNULL(ipo.NoProduksi,'') LIKE @search
               OR ISNULL(m2.NamaMesin,'') LIKE @search
               OR ISNULL(bs.NoBongkarSusun,'') LIKE @search
             )`
          : ''
      }
    GROUP BY
      b.NoBonggolan, b.DateCreate, b.IdBonggolan, mb.NamaBonggolan,
      b.IdWarehouse, w.NamaWarehouse, b.Blok, b.IdLokasi, b.Berat, b.IdStatus
    ORDER BY b.NoBonggolan DESC
    OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY;
  `;

  const countQuery = `
    SELECT COUNT(DISTINCT b.NoBonggolan) AS total
    FROM [dbo].[Bonggolan] b
    LEFT JOIN [dbo].[MstBonggolan] mb
           ON mb.IdBonggolan = b.IdBonggolan
    LEFT JOIN [dbo].[MstWarehouse] w
           ON w.IdWarehouse = b.IdWarehouse
    LEFT JOIN [dbo].[BrokerProduksiOutputBonggolan] bpo
           ON bpo.NoBonggolan = b.NoBonggolan
    LEFT JOIN [dbo].[BrokerProduksi_h] bp
           ON bp.NoProduksi = bpo.NoProduksi
    LEFT JOIN [dbo].[MstMesin] m1
           ON m1.IdMesin = bp.IdMesin
    LEFT JOIN [dbo].[InjectProduksiOutputBonggolan] ipo
           ON ipo.NoBonggolan = b.NoBonggolan
    LEFT JOIN [dbo].[InjectProduksi_h] ip
           ON ip.NoProduksi = ipo.NoProduksi
    LEFT JOIN [dbo].[MstMesin] m2
           ON m2.IdMesin = ip.IdMesin
    LEFT JOIN [dbo].[BongkarSusunOutputBonggolan] bs
           ON bs.NoBonggolan = b.NoBonggolan
    WHERE 1=1
      AND b.DateUsage IS NULL
      ${
        search
          ? `AND (
               b.NoBonggolan LIKE @search
               OR b.Blok LIKE @search
               OR CONVERT(VARCHAR(20), b.IdLokasi) LIKE @search
               OR CONVERT(VARCHAR(20), b.IdWarehouse) LIKE @search
               OR ISNULL(w.NamaWarehouse,'') LIKE @search
               OR ISNULL(mb.NamaBonggolan,'') LIKE @search      -- 🆕 searchable
               OR ISNULL(bpo.NoProduksi,'') LIKE @search
               OR ISNULL(m1.NamaMesin,'') LIKE @search
               OR ISNULL(ipo.NoProduksi,'') LIKE @search
               OR ISNULL(m2.NamaMesin,'') LIKE @search
               OR ISNULL(bs.NoBongkarSusun,'') LIKE @search
             )`
          : ''
      }
  `;

  request.input('offset', sql.Int, offset);
  request.input('limit', sql.Int, limit);
  if (search) request.input('search', sql.VarChar, `%${search}%`);

  const [dataResult, countResult] = await Promise.all([
    request.query(baseQuery),
    request.query(countQuery),
  ]);

  const data = dataResult.recordset || [];
  const total = countResult.recordset?.[0]?.total ?? 0;

  return { data, total };
};





function padLeft(num, width) {
       const s = String(num);
       return s.length >= width ? s : '0'.repeat(width - s.length) + s;
     }
     
     // Generate next NoBonggolan: e.g. 'M.0000000002'
     async function generateNextNoBonggolan(tx, { prefix = 'M.', width = 10 } = {}) {
       const rq = new sql.Request(tx);
       const q = `
         SELECT TOP 1 b.NoBonggolan
         FROM [dbo].[Bonggolan] AS b WITH (UPDLOCK, HOLDLOCK)
         WHERE b.NoBonggolan LIKE @prefix + '%'
         ORDER BY TRY_CONVERT(BIGINT, SUBSTRING(b.NoBonggolan, LEN(@prefix) + 1, 50)) DESC,
                  b.NoBonggolan DESC;
       `;
       const r = await rq.input('prefix', sql.VarChar, prefix).query(q);
     
       let lastNum = 0;
       if (r.recordset.length > 0) {
         const last = r.recordset[0].NoBonggolan;       // e.g. "M.0000000001"
         const numericPart = last.substring(prefix.length);
         lastNum = parseInt(numericPart, 10) || 0;
       }
       const next = lastNum + 1;
       return prefix + padLeft(next, width);
     }
     
     exports.createBonggolanCascade = async (payload) => {
       const pool = await poolPromise;
       const tx = new sql.Transaction(pool);
     
       const header = payload?.header || {};
       const processedCode = (payload?.ProcessedCode || '').toString().trim(); // may be '', 'E.****', 'S.****', 'BG.****'
     
       // ---- validation
       const badReq = (msg) => {
         const e = new Error(msg);
         e.statusCode = 400;
         return e;
       };
       if (!header.IdBonggolan) throw badReq('IdBonggolan is required');
       if (!header.IdWarehouse) throw badReq('IdWarehouse is required');
       if (!header.CreateBy) throw badReq('CreateBy is required');
     
       // Identify target from ProcessedCode (optional)
       const hasProcessed = processedCode.length > 0;
       let processedType = null; // 'BROKER' | 'INJECT' | 'BONGKAR'
       if (hasProcessed) {
         if (processedCode.startsWith('E.')) processedType = 'BROKER';
         else if (processedCode.startsWith('S.')) processedType = 'INJECT';
         else if (processedCode.startsWith('BG.')) processedType = 'BONGKAR';
         else throw badReq('ProcessedCode prefix not recognized (use E., S., or BG.)');
       }
     
       try {
         await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);
     
         // 1) Generate NoBonggolan
         const generatedNo = await generateNextNoBonggolan(tx, { prefix: 'M.', width: 10 });
     
         // Double-check uniqueness
         const rqCheck = new sql.Request(tx);
         const exist = await rqCheck
           .input('NoBonggolan', sql.VarChar, generatedNo)
           .query(`SELECT 1 FROM [dbo].[Bonggolan] WITH (UPDLOCK, HOLDLOCK) WHERE NoBonggolan = @NoBonggolan`);
     
         const noBonggolan = (exist.recordset.length > 0)
           ? await generateNextNoBonggolan(tx, { prefix: 'M.', width: 10 }) // regenerate (very rare)
           : generatedNo;
     
         // 2) Insert header into PPS_TEST2.dbo.Bonggolan
         const nowDateOnly = header.DateCreate || null; // if null -> GETDATE() (date)
         const insertHeaderSql = `
           INSERT INTO [dbo].[Bonggolan] (
             NoBonggolan, DateCreate, IdBonggolan, IdWarehouse, DateUsage,
             Berat, IdStatus, Blok, IdLokasi, CreateBy, DateTimeCreate
           )
           VALUES (
             @NoBonggolan,
             ${nowDateOnly ? '@DateCreate' : 'CONVERT(date, GETDATE())'},
             @IdBonggolan, @IdWarehouse, NULL,
             @Berat, @IdStatus, @Blok, @IdLokasi, @CreateBy, GETDATE()
           );
         `;
     
         const rqHeader = new sql.Request(tx);
         rqHeader
           .input('NoBonggolan', sql.VarChar, noBonggolan)
           .input('IdBonggolan', sql.Int, header.IdBonggolan)
           .input('IdWarehouse', sql.Int, header.IdWarehouse)
           .input('Berat', sql.Decimal(18, 3), header.Berat ?? null)
           .input('IdStatus', sql.Int, header.IdStatus ?? 1) // default 1
           .input('Blok', sql.VarChar, header.Blok ?? null)
           .input('IdLokasi', sql.VarChar, header.IdLokasi ?? null)
           .input('CreateBy', sql.VarChar, header.CreateBy);
     
         if (nowDateOnly) rqHeader.input('DateCreate', sql.Date, new Date(nowDateOnly));
         await rqHeader.query(insertHeaderSql);
     
         // 3) Optional: insert mapping based on ProcessedCode prefix
         let mappingTable = null;
         if (processedType === 'BROKER') {
           // E. → BrokerProduksiOutputBonggolan
           const q = `
             INSERT INTO [dbo].[BrokerProduksiOutputBonggolan] (NoProduksi, NoBonggolan)
             VALUES (@Processed, @NoBonggolan);
           `;
           await new sql.Request(tx)
             .input('Processed', sql.VarChar, processedCode)
             .input('NoBonggolan', sql.VarChar, noBonggolan)
             .query(q);
           mappingTable = 'BrokerProduksiOutputBonggolan';
         } else if (processedType === 'INJECT') {
           // S. → InjectProduksiOutputBonggolan
           const q = `
             INSERT INTO [dbo].[InjectProduksiOutputBonggolan] (NoProduksi, NoBonggolan)
             VALUES (@Processed, @NoBonggolan);
           `;
           await new sql.Request(tx)
             .input('Processed', sql.VarChar, processedCode)
             .input('NoBonggolan', sql.VarChar, noBonggolan)
             .query(q);
           mappingTable = 'InjectProduksiOutputBonggolan';
         } else if (processedType === 'BONGKAR') {
           // BG. → BongkarSusunOutputBonggolan
           const q = `
             INSERT INTO [dbo].[BongkarSusunOutputBonggolan] (NoBongkarSusun, NoBonggolan)
             VALUES (@Processed, @NoBonggolan);
           `;
           await new sql.Request(tx)
             .input('Processed', sql.VarChar, processedCode)
             .input('NoBonggolan', sql.VarChar, noBonggolan)
             .query(q);
           mappingTable = 'BongkarSusunOutputBonggolan';
         }
     
         await tx.commit();
     
         return {
           header: {
             NoBonggolan: noBonggolan,
             DateCreate: nowDateOnly || 'GETDATE()',
             IdBonggolan: header.IdBonggolan,
             IdWarehouse: header.IdWarehouse,
             Berat: header.Berat ?? null,
             IdStatus: header.IdStatus ?? 1,
             Blok: header.Blok ?? null,
             IdLokasi: header.IdLokasi ?? null,
             CreateBy: header.CreateBy,
             DateTimeCreate: 'GETDATE()',
           },
           processed: {
             code: processedCode || null,
             type: processedType,       // BROKER / INJECT / BONGKAR / null
             mappingTable,              // which table got the insert, if any
           },
         };
       } catch (e) {
         try { await tx.rollback(); } catch (_) {}
         throw e;
       }
     };



     exports.updateBonggolan = async (noBonggolan, payload = {}) => {
       const pool = await poolPromise;
       const tx = new sql.Transaction(pool);
     
       const fields = [
         { key: 'DateCreate',  type: sql.Date },
         { key: 'IdBonggolan', type: sql.Int },
         { key: 'IdWarehouse', type: sql.Int },
         { key: 'DateUsage',   type: sql.Date },
         { key: 'Berat',       type: sql.Decimal(18, 3) },
         { key: 'IdStatus',    type: sql.Int },
         { key: 'Blok',        type: sql.VarChar },
         { key: 'IdLokasi',    type: sql.VarChar },
       ];
     
       const toUpdate = fields.filter(f => payload[f.key] !== undefined);
       if (toUpdate.length === 0) {
         const e = new Error('No valid fields to update');
         e.statusCode = 400;
         throw e;
       }
     
       try {
         await tx.begin(sql.ISOLATION_LEVEL.READ_COMMITTED);
     
         const exists = await new sql.Request(tx)
           .input('NoBonggolan', sql.VarChar, noBonggolan)
           .query(`
             SELECT 1
             FROM [PPS_TEST2].[dbo].[Bonggolan] WITH (UPDLOCK, HOLDLOCK)
             WHERE NoBonggolan = @NoBonggolan
           `);
     
         if (exists.recordset.length === 0) {
           await tx.rollback();
           const e = new Error(`Bonggolan not found: ${noBonggolan}`);
           e.statusCode = 404;
           throw e;
         }
     
         const setClauses = [];
         const rq = new sql.Request(tx);
         rq.input('NoBonggolan', sql.VarChar, noBonggolan);
     
         for (const f of toUpdate) {
           const p = `p_${f.key}`;
           setClauses.push(`[${f.key}] = @${p}`);
     
           if (f.type === sql.Date && payload[f.key]) {
             rq.input(p, f.type, new Date(payload[f.key]));
           } else if (f.type.declaration?.startsWith('decimal') && payload[f.key] !== null) {
             rq.input(p, f.type, Number(payload[f.key]));
           } else {
             rq.input(p, f.type, payload[f.key]);
           }
         }
     
         await rq.query(`
           UPDATE [PPS_TEST2].[dbo].[Bonggolan]
           SET ${setClauses.join(', ')}
           WHERE NoBonggolan = @NoBonggolan;
         `);
     
         await tx.commit();
     
         return { updated: true, updatedFields: toUpdate.map(f => f.key) };
       } catch (err) {
         try { await tx.rollback(); } catch (_) {}
         throw err;
       }
     };



     exports.deleteBonggolanCascade = async (noBonggolan) => {
       const pool = await poolPromise;
       const tx = new sql.Transaction(pool);
     
       try {
         await tx.begin(sql.ISOLATION_LEVEL.READ_COMMITTED);
     
         // Step 1: delete mappings if exist
         const mappingQueries = [
           `DELETE FROM [dbo].[BrokerProduksiOutputBonggolan] WHERE NoBonggolan = @NoBonggolan`,
           `DELETE FROM [dbo].[InjectProduksiOutputBonggolan] WHERE NoBonggolan = @NoBonggolan`,
           `DELETE FROM [dbo].[BongkarSusunOutputBonggolan] WHERE NoBonggolan = @NoBonggolan`,
         ];
     
         for (const q of mappingQueries) {
           await new sql.Request(tx)
             .input('NoBonggolan', sql.VarChar, noBonggolan)
             .query(q);
         }
     
         // Step 2: delete header from Bonggolan
         const delHeader = `
           DELETE FROM [dbo].[Bonggolan]
           WHERE NoBonggolan = @NoBonggolan;
         `;
         const result = await new sql.Request(tx)
           .input('NoBonggolan', sql.VarChar, noBonggolan)
           .query(delHeader);
     
         await tx.commit();
     
         if (result.rowsAffected[0] === 0) {
           const e = new Error(`Bonggolan not found: ${noBonggolan}`);
           e.statusCode = 404;
           throw e;
         }
     
         return { deleted: true, noBonggolan };
       } catch (err) {
         try { await tx.rollback(); } catch (_) {}
         throw err;
       }
     };