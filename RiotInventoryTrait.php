<?php namespace Riot\Inventory\Traits;

use App\LFInventoryRawData;
use App\LogisticInventorySynchronous;
use App\RiotInventory;
use App\Utilities\ParamSigner;
use Carbon\Carbon;
use Riot\Inbound\Models\Inbound;
use Riot\RiotIms\Models\LogisticOrder;
use Riot\RiotIms\Models\LogisticOrderCancellation;
use Riot\RiotIms\Models\LogisticOrderDelivery;
use Riot\RiotIms\Models\LogisticReceiveArrival;
use Riot\Warehouse\Models\WarehouseReceipt;

trait RiotInventoryTrait
{
    protected $statistics         = [];
    protected $deliveryItems      = [];
    protected $sku;
    protected $plantId;
    protected $date;
    protected $shipmentItems      = [];
    protected $arrivals           = [];
    protected $adjustInItems      = [];
    protected $adjustOutItems     = [];
    protected $lastDayInventories = [];
    protected $lastDate;
    protected $lastTwoDate;

    /**
     * @throws \Exception
     */
    protected function statistic($sku = null, $date, $plant_id = null)
    {
        try {
            if (!$plant_id || !$date) {
                throw new \Exception('Missing PlantID or Date.');
            }
            $this->statistics = [];
            $this->plantId = $plant_id;
            if ($sku) $this->sku = $sku;
            //命令行日期
            $this->date = $date;
            //命令行日期 昨天
            $this->lastDate = Carbon::createFromFormat('Y-m-d', $date)->addDay(-1)->format('Y-m-d');
            //命令行日期 前天
            $this->lastTwoDate = Carbon::createFromFormat('Y-m-d', $date)->addDay(-2)->format('Y-m-d');

            //获取昨天出库单
            $this->shipments();
            $this->collectItemAndCount($this->shipmentItems, 'pushed');
            if ($this->plantId == 'PY05') {
                //获取昨天退货
                $this->b2cReturns();
                $this->collectItemAndCount($this->arrivals, 'returned');
            } else {
                $this->b2bReturns();
            }

            //获取昨天出库
            $this->deliveries();
            $this->collectItemAndCount($this->deliveryItems, 'delivered');
            //获取昨天库存调出
            $this->adjust('deductionItems');
            $this->collectItemAndCount($this->adjustOutItems, 'adjust_out');
            //获取昨天库存调入
            $this->adjust('additionItems');
            $this->collectItemAndCount($this->adjustInItems, 'adjust_in');
            if ($this->plantId == 'PY05S') {
                //获取昨天入库
                $this->inbounds();
            }
            //获取前天的riot库存
            if (!$this->lastRiotInventory()) {
                //如果没有前天没有riot库存，获取昨天仓库库存
                $this->lastDayLFInventory();
            }
            $this->riotInventory();
            $this->compare();

            return $this->statistics;
        } catch (\Exception $e) {
            throw $e;
        }
    }

    private function b2bReturns()
    {
        $query = Inbound::query()
            ->where('receipt_date', '>=', $this->lastDate . ' 00:00:00')
            ->where('receipt_date', '<=', $this->lastDate . ' 23:59:59')
            ->where('inbound_type', 'OFFLINE_RETURN')
            ->where('facility', 'PY05S');
        if ($this->sku) {
            $query->where('sku', $this->sku);
        }
        $b2bArrivals = $query->get();
        $type = 'returned';

        foreach ($b2bArrivals as $item) {
            if ($this->sku && strtoupper($item->sku) != $this->sku) {
                continue;
            }
            if (key_exists(strtoupper($item->sku), $this->statistics)) {
                if (key_exists($type, $this->statistics[strtoupper($item->sku)])) {
                    $this->statistics[strtoupper($item->sku)][$type] += $item->qty_received_piece;
                }
                if (!key_exists($type, $this->statistics[strtoupper($item->sku)])) {
                    $this->statistics[strtoupper($item->sku)][$type] = $item->qty_received_piece;
                }
            }
            if (!key_exists(strtoupper($item->sku), $this->statistics)) {
                $this->statistics[strtoupper($item->sku)][$type] = $item->qty_received_piece;
            }
        }
    }

    private function inbounds()
    {
        $inbounds = Inbound::query()
            ->where('rectype', 'Normal ASN')
            ->where('receipt_date', '>=', $this->lastDate . ' 00:00:00')
            ->where('receipt_date', '<=', $this->lastDate . ' 23:59:59')
            ->where('inbound_type', 'NEW_INBOUND')
            ->groupBy(['extern_receipt_key', 'host_wh_code', 'sku', 'facility', 'qty_received_piece', 'lottable_02'])
            ->get();

        foreach ($inbounds as $inbound) {
            if ($this->sku && $inbound->sku != $this->sku) {
                continue;
            }
            if (key_exists($inbound->sku, $this->statistics) && key_exists('inbound', $this->statistics[$inbound->sku])) {
                $this->statistics[$inbound->sku]['inbound'] += $inbound->qty_received_piece;
            }
            if (!key_exists($inbound->sku, $this->statistics) || !key_exists('inbound', $this->statistics[$inbound->sku])) {
                $this->statistics[$inbound->sku]['inbound'] = $inbound->qty_received_piece;
            }
        }
    }

    private function compare()
    {
        $timeStart = Carbon::createFromFormat('Y-m-d', $this->date)->format('Y-m-d 00:00:00');
        $timeEnd = Carbon::createFromFormat('Y-m-d', $this->date)->format('Y-m-d 01:00:00');
        $dailyFirstInventory = LFInventoryRawData::query()
            ->where('created_at', '>', $timeStart)
            ->where('created_at', '<', $timeEnd)
            ->orderBy('id')
            ->first();
        $batchCode = null;
        if ($dailyFirstInventory) {
            $batchCode = $dailyFirstInventory->batch_code;
        }
        $query = <<<SQL
SELECT sku, SUM(qty_on_hand) AS hand_qty FROM lf_inventory_raw_datas 
WHERE 
      inventory_date='$this->date'
AND 
      plant_id='$this->plantId'
SQL;
        if (!$batchCode) {
            $query .= <<<SQL
AND 
      created_at < '$timeEnd' 
AND 
        batch_code IS NULL 
SQL;
        } else {
            $query .= <<<SQL
AND 
      batch_code = '$batchCode' 
SQL;
        }
        if ($this->sku) {
            $query .= <<<SQL
AND sku='$this->sku'
SQL;
        }
        $query .= <<<SQL
GROUP BY sku
SQL;
        $latestInventories = \DB::select($query);
        foreach ($latestInventories as $latestInventory) {
            if (key_exists(strtoupper($latestInventory->sku), $this->statistics)) {
                $this->statistics[strtoupper($latestInventory->sku)]['lf_daily_end_qty'] = $latestInventory->hand_qty * 1;
                $this->statistics[strtoupper($latestInventory->sku)]['difference'] = $this->statistics[strtoupper($latestInventory->sku)]['qty_on_hand'] * 1 - $latestInventory->hand_qty * 1;
                $this->statistics[strtoupper($latestInventory->sku)]['is_match'] = $this->statistics[strtoupper($latestInventory->sku)]['difference'] == 0;
            }
            if (!key_exists(strtoupper($latestInventory->sku), $this->statistics)) {
                $this->statistics[strtoupper($latestInventory->sku)] = [
                    'qty_on_hand' => $latestInventory->hand_qty,
                    'difference'  => 0,
                    'is_match'    => 1
                ];
            }
        }

        $deleteQuery = RiotInventory::query()
            ->where('plant_id', $this->plantId);
        if ($this->sku) {
            $deleteQuery->where('sku', $this->sku);
        }
        $deleteQuery->where('inventory_date', $this->lastDate)
            ->delete();

        $insertData = [];
        foreach ($this->statistics as $sku => $statistic) {
            if (strpos($sku, 'LAMP') != false) {
                unset($this->statistics[$sku]);
                continue;
            }
            $insertData[] = [
                'sku'              => $sku,
                'inventory_date'   => $this->lastDate,
                'plant_id'         => $this->plantId,
                'qty_on_hand'      => array_get($statistic, 'last_day_start_qty') && array_get($statistic, 'last_day_start_qty') >= 0 ? array_get($statistic, 'last_day_start_qty') : 0,
                'pushed'           => array_get($statistic, 'pushed') ?: 0,
                'delivered'        => array_get($statistic, 'delivered') ?: 0,
                'returned'         => array_get($statistic, 'returned') ?: 0,
                'inbound'          => array_get($statistic, 'inbound') ?: 0,
                'adjust_in'        => array_get($statistic, 'adjust_in') ?: 0,
                'adjust_out'       => array_get($statistic, 'adjust_out') ?: 0,
                'is_match'         => array_get($statistic, 'is_match') ?: (array_get($statistic, 'difference') == 0),
                'difference'       => array_get($statistic, 'difference') ?: 0,
                'daily_end_qty'    => array_get($statistic, 'qty_on_hand') ?: 0,
                'lf_daily_end_qty' => array_get($statistic, 'lf_daily_end_qty') ?: 0,
                'created_at'       => Carbon::now()->format('Y-m-d H:i:s'),
                'updated_at'       => Carbon::now()->format('Y-m-d H:i:s'),
            ];
        }
        \DB::table('riot_inventories')->insert($insertData);
    }

    private function lastRiotInventory()
    {
        $query = RiotInventory::query();

        if ($this->sku) {
            $query->where('sku', $this->sku);
        }
        $query->where('inventory_date', $this->lastTwoDate)
            ->where('plant_id', $this->plantId);

        $this->lastDayInventories = $query->get();
        if (count($this->lastDayInventories) <= 0) {
            return false;
        }
        foreach ($this->lastDayInventories as $lastDayInventory) {
            if (key_exists(strtoupper($lastDayInventory->sku), $this->statistics)) {
                $this->statistics[strtoupper($lastDayInventory->sku)]['last_day_start_qty'] = $lastDayInventory->daily_end_qty;
            }
            if (!key_exists(strtoupper($lastDayInventory->sku), $this->statistics)) {
                $this->statistics[strtoupper($lastDayInventory->sku)]['last_day_start_qty'] = $lastDayInventory->daily_end_qty;
            }
        }
        return true;
    }

    private function riotInventory()
    {
        foreach ($this->statistics as $sku => $statistic) {
            if (!key_exists('delivered', $statistic)) {
                $statistic['delivered'] = 0;
            }
            if (!key_exists('last_day_start_qty', $statistic)) {
                $statistic['last_day_start_qty'] = 0;
            }
            if (!key_exists('pushed', $statistic)) {
                $statistic['pushed'] = 0;
            }
            if (!key_exists('adjust_in', $statistic)) {
                $statistic['adjust_in'] = 0;
            }
            if (!key_exists('adjust_out', $statistic)) {
                $statistic['adjust_out'] = 0;
            }
            if (!key_exists('returned', $statistic)) {
                $statistic['returned'] = 0;
            }
            if (!key_exists('inbound', $statistic)) {
                $statistic['inbound'] = 0;
            }
            $basicHandQty = $statistic['last_day_start_qty']
                + $statistic['returned']
                + $statistic['adjust_in']
                + $statistic['inbound']
                + $statistic['adjust_out'];

            $statistic['qty_on_hand'] = $basicHandQty - $statistic['delivered'];

            $this->statistics[$sku] = $statistic;
        }
    }

    private function shipments()
    {
        $query = LogisticOrder::query()
            ->where('created_at', '>=', $this->lastDate . ' 00:00:00')
            ->where('created_at', '<=', $this->lastDate . ' 23:59:59')
            ->where('is_cancelled', 0)
            ->where('plant_id', $this->plantId)
            ->with('items');
        if ($this->sku) {
            $query->whereHas('items', function ($q) {
                $q->where('item_id', $this->sku);
            });
        }
        $query->whereHas('_shipment_status', function ($q) {
            $q->where('status', 'WMS_ACCEPT');
        });
        $this->shipmentItems = $query->get();
    }

    private function deliveries()
    {
        $dateStr = Carbon::createFromFormat('Y-m-d', $this->lastDate)->format('Ymd');
        $query = LogisticOrderDelivery::query()
            ->where('completed_date', 'like', $dateStr . '%')
            ->where('plant_id', $this->plantId)
            ->with('items');
        if ($this->sku) {
            $query->whereHas('items', function ($q) {
                $q->where('item_id', $this->sku);
            });
        }
        $this->deliveryItems = $query->get();
    }

    private function collectItemAndCount($bills, $type)
    {
        foreach ($bills as $order) {
            foreach ($order->items as $item) {
                if ($this->sku && strtoupper($item->item_id) != $this->sku) {
                    continue;
                }
                $qty = $item->item_qty;
                if ($type == 'adjust_out') {
                    $qty = $item->item_qty;
                    if ($qty > 0) {
                        $qty = 0 - $qty;
                    }
                }
                if ($type == 'adjust_in') {
                    $qty = $item->item_qty;
                    if ($qty < 0) {
                        $qty = 0 - $qty;
                    }
                }
                if (key_exists(strtoupper($item->item_id), $this->statistics)) {
                    if (key_exists($type, $this->statistics[strtoupper($item->item_id)])) {
                        $this->statistics[strtoupper($item->item_id)][$type] += $qty;
                    }
                    if (!key_exists($type, $this->statistics[strtoupper($item->item_id)])) {
                        $this->statistics[strtoupper($item->item_id)][$type] = $qty;
                    }
                }
                if (!key_exists(strtoupper($item->item_id), $this->statistics)) {
                    $this->statistics[strtoupper($item->item_id)][$type] = $qty;
                }
            }
        }
    }

    private function adjust($type)
    {
        if (!in_array($type, ['deductionItems', 'additionItems'])) {
            throw new \Exception('Wrong adjustments association.');
        }
        $date = Carbon::createFromFormat('Y-m-d', $this->lastDate)->format('Ymd');
        $query = LogisticInventorySynchronous::query()
            ->with($type);
        if ($type == 'deductionItems') {
            $query->where('plant_id', $this->plantId);
        }
        if ($type == 'additionItems') {
            $query->where(function ($q) {
                $q->where('to_plant_id', $this->plantId)
                    ->orWhere(function ($sq) {
                        $sq->where('to_plant_id', '')
                            ->where('plant_id', $this->plantId);
                    });
            });
        }

        $query->where('completed_date', 'like', $date . '%');
        if ($this->sku) {
            $query->whereHas('items', function ($q) {
                $q->where('item_id', $this->sku);
            });
        }
        if ($type == 'deductionItems') {
            $this->adjustOutItems = $query->get();
            foreach ($this->adjustOutItems as $adjustOutItem) {
                $adjustOutItem->items = $adjustOutItem->deductionItems;
            }
        }
        if ($type == 'additionItems') {
            $this->adjustInItems = $query->get();
            foreach ($this->adjustInItems as $adjustInItem) {
                $adjustInItem->items = $adjustInItem->additionItems;
            }
        }
    }

    private function b2cReturns()
    {
        $date = Carbon::createFromFormat('Y-m-d', $this->lastDate)->format('Ymd');
        $query = LogisticReceiveArrival::query()
            ->where('completed_date', 'like', $date . '%')
            ->with('items');
        if ($this->sku) {
            $query->whereHas('items', function ($q) {
                $q->where('item_id', $this->sku);
            });
        }
        $this->arrivals = $query->get();
    }

    private function lastDayLFInventory()
    {
        $query = <<<SQL
SELECT sku, SUM(qty_on_hand) AS hand_qty FROM lf_inventory_raw_datas 
WHERE 
      inventory_date='$this->lastDate'
AND 
      plant_id='$this->plantId'
SQL;
        if ($this->sku) {
            $query .= <<<SQL
AND sku='$this->sku'
SQL;
        }
        $query .= <<<SQL
GROUP BY sku
SQL;

        $this->lastDayInventories = \DB::select($query);

        foreach ($this->lastDayInventories as $lastDayInventory) {
            if (key_exists(strtoupper($lastDayInventory->sku), $this->statistics)) {
                $this->statistics[strtoupper($lastDayInventory->sku)]['last_day_start_qty'] = $lastDayInventory->hand_qty * 1;
            }
        }
    }
}
