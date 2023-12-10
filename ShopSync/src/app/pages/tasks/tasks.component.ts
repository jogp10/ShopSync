import { Component, OnInit } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';
import { ShoppingListService } from 'src/app/shared/services/shopping-list.service';

@Component({
  selector: 'app-tasks',
  templateUrl: './tasks.component.html',
  // styleUrls: ['./tasks.component.scss']
})
export class TasksComponent implements OnInit {
  dataSource: any[] = [];
  list_title: string = '';
  list_id: string = '';

  constructor(private router: Router, private route: ActivatedRoute, private shoppingListService: ShoppingListService) {}

  ngOnInit() {
    this.route.paramMap.subscribe(params => {
      this.list_id = params.get('task_id') || '';

      this.shoppingListService.getShoppingList(this.list_id).subscribe({
        next: (data: any) => {
          this.list_title = data.name;
          console.log('data', data);
          this.generateDataSource(data);
        },
      });
    });
  }

  generateDataSource(shoppingList: any) {
    // Assuming your shoppingList has an 'items' array
    this.dataSource = shoppingList.items.map((item: any) => {
      if (item.quantity == 0) {
        return null;
      }
      return {
        item: item.item,
        quantity: item.quantity,
      };
    });
  }

  onCellClick(event: any) {
    // Check if the clicked cell is in the "Quantity" column
    if (event.column.dataField === 'quantity') {
      // Set the editing state for the clicked cell
      event.component.editCell(event.rowIndex, 'quantity');
    }
  }  

  // Define the onEditingStart method to handle the editing start event
  onRowUpdated(event: any) {
    const item = event.key;
    const newQuantity = event.data.quantity;

    // Make your API call using your service
    this.shoppingListService.updateShoppingListItem(this.list_id, item, newQuantity).subscribe({
      next: (data: any) => {
        console.log('API call successful', data);
        // Optionally, you can update the grid or perform other actions upon success
        this.generateDataSource(data);
      },
      error: (error: any) => {
        console.error('API call failed', error);
        // Handle errors as needed
      },
    });

  }

  onRowInserted(event: any) {
    // Extract the new row data
    const newRowData = event.data;

    console.log('newRowData', newRowData);
    if (newRowData.quantity == 0) {
      newRowData.quantity = 1;
    }
  
    // Make your API call here using the newRowData
    this.shoppingListService.addShoppingListItem(this.list_id, newRowData.item, newRowData.quantity).subscribe({
      next: (data: any) => {
        console.log('API call successful', data);
        // Optionally, you can update the grid or perform other actions upon success
        this.generateDataSource(data);
      },
      error: (error: any) => {
        console.error('API call failed', error);
        // Handle errors as needed
      },
    });
  }

  onRowRemoved(event: any) {
    // Extract the deleted row data
    const deletedRowData = event.data;
    console.log('deletedRowData', deletedRowData);
  
    // Make your API call here using the deletedRowData
    this.shoppingListService.deleteShoppingListItem(this.list_id, deletedRowData.item).subscribe({
      next: (data: any) => {
        console.log('API call successful', data);
        // Optionally, you can update the grid or perform other actions upon success
        this.generateDataSource(data);
      },
      error: (error: any) => {
        console.error('API call failed', error);
        // Handle errors as needed
      },
    });
  }

  loadShoppingList() {
    // Implement logic to load shopping list from the cloud
    console.log('Load Shopping List clicked');

    this.shoppingListService.getShoppingListFromCloud(this.list_id).subscribe({
      next: (data: any) => {
        console.log('data', data);
        this.generateDataSource(data);
      },
    });
  }
  
  storeShoppingList() {
    // Implement logic to store shopping list in the cloud
    console.log('Store Shopping List clicked');

    this.shoppingListService.storeShoppingListToCloud(this.list_id).subscribe({
      next: (data: any) => {
        console.log('data', data);
        this.generateDataSource(data);
      },
    });
  }
}
