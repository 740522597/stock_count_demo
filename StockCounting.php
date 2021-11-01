<?php

namespace App\Console\Commands;

use App\LFInventoryRawData;
use App\LogisticInventorySynchronous;
use App\RiotInventory;
use Carbon\Carbon;
use Illuminate\Console\Command;
use Riot\Inventory\Traits\RiotInventoryTrait;
use Riot\RiotIms\Models\LogisticOrderDelivery;
use Riot\Traits\EmailErrorNotifyTrait;

class StockCounting extends Command
{
    use RiotInventoryTrait, EmailErrorNotifyTrait;
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'stock:counting {{--plant_id=}} {{--date=}} {{--from=}} {{--to=}} {{--sku=}}';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Stock counting with type "b2c" "b2b" and date with format "2021-07-02"';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        try {
            $plantId = $this->option('plant_id');
            $date = $this->option('date');
            $sku = $this->option('sku');
            $from = $this->option('from');
            $to = $this->option('to');

            if ($from && $to) {
                $date = $from;
                while (Carbon::createFromFormat('Y-m-d', $date)->lte(Carbon::createFromFormat('Y-m-d', $to))) {
                    $this->running($date, $plantId, $sku);
                    $date = Carbon::createFromFormat('Y-m-d', $date)->addDay(1)->format('Y-m-d');
                }
            } else {
                $this->running($date, $plantId, $sku);
            }
            $this->checkUnmatch();

            $this->unmatchedCheck();
        } catch (\Exception $e) {
            $this->sendEmailErrorNotify('库存核对时发现异常', $e);
            dd_error($e);
        }
    }

    protected function unmatchedCheck()
    {
        try {
            $unmatchedItems = RiotInventory::query()
                ->where('is_match', 0)
                ->orderBy('inventory_date')
                ->get();

            foreach ($unmatchedItems as $unmatchedItem) {
                $this->running(Carbon::createFromFormat('Y-m-d', $unmatchedItem->inventory_date)->addDay(-1)->format('Y-m-d'), $unmatchedItem->plant_id, $unmatchedItem->sku);
            }
        } catch (\Exception $e) {
            throw $e;
        }
    }

    protected function running($date, $plantId=null, $sku=null)
    {
        try {
            if (!$date) {
                $date = Carbon::now()->format('Y-m-d');
            }

            $this->line('Querying inventory ' . $date);

            if (!$plantId) {
                $plantIds = ['PY05', 'PY05S'];
            } else {
                $plantIds = [$plantId];
            }
            foreach ($plantIds as $plantId) {
                if (!in_array($plantId, ['PY05', 'PY05S'])) {
                    throw new \Exception('Plant ID not exist.');
                }
                $statistics = $this->statistic($sku, $date, $plantId);
                if ($sku) {
                    dd($statistics);
                }
            }
        } catch (\Exception $e) {
            throw $e;
        }
    }

    protected function checkUnmatch($date=null)
    {
        $count = RiotInventory::query()
            ->where('is_match', 0)
            ->where('inventory_date', Carbon::now()->format('Y-m-d'))
            ->count();
        if ($count) {
            throw new \Exception($date . ' has un-matched inventory items, please have a check.');
        }
    }
}
