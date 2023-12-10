import { Component, NgModule, Output, Input, EventEmitter, ViewChild, ElementRef, AfterViewInit, OnDestroy } from '@angular/core';
import { DxTreeViewModule, DxTreeViewComponent, DxTreeViewTypes } from 'devextreme-angular/ui/tree-view';
import * as events from 'devextreme/events';
import { ShoppingListService } from '../../services/shopping-list.service';
import notify from 'devextreme/ui/notify';
import { Router } from '@angular/router';

@Component({
  selector: 'app-side-navigation-menu',
  templateUrl: './side-navigation-menu.component.html',
  styleUrls: ['./side-navigation-menu.component.scss']
})
export class SideNavigationMenuComponent implements AfterViewInit, OnDestroy {
  @ViewChild(DxTreeViewComponent, { static: true })
  menu!: DxTreeViewComponent;
  private menuItems: any[] = [];


  @Output()
  selectedItemChanged = new EventEmitter<DxTreeViewTypes.ItemClickEvent>();

  @Output()
  openMenu = new EventEmitter<any>();

  private _selectedItem!: String;
  @Input()
  set selectedItem(value: String) {
    this._selectedItem = value;
    if (!this.menu.instance) {
      return;
    }

    this.menu.instance.selectItem(value);
  }

  private _items!: Record <string, unknown>[];
  get items() {
    if (!this._items) {
      try {
        this.shoppingListService.getListNames().subscribe({
          next: (data: any) => {
            this._items = this.getItems(data);
          },
          error: (error: any) => {
            console.error('Error fetching dynamic navigation', error);
          },
        });
        // Additional processing if needed
      } catch (error) {
        console.error('Error fetching dynamic navigation', error);
      }
    }
  
    return this._items || [];
  }

  private _compactMode = false;
  @Input()
  get compactMode() {
    return this._compactMode;
  }
  set compactMode(val) {
    this._compactMode = val;

    if (!this.menu.instance) {
      return;
    }

    if (val) {
      this.menu.instance.collapseAll();
    } else {
      this.menu.instance.expandItem(this._selectedItem);
    }
  }

  constructor(private elementRef: ElementRef, private shoppingListService: ShoppingListService, private router: Router) { }

  onItemClick(event: DxTreeViewTypes.ItemClickEvent) {
    this.selectedItemChanged.emit(event);
  }

  private getItems(data: any): any[] {
    console.log('data', data);
    this.menuItems = data;

    // Add a new item for creating a list
    const createList = {
      text: 'Create New List',
      icon: 'plus',
      onClick: () => {
        // Use window.prompt to get the new list name from the user
        const newListName = prompt('Enter the name for the new list:');
        
        // Check if the user entered a name
        if (newListName !== null && newListName.trim() !== '') {
          // Handle the click event to create the new list
          console.log('Create New List:', newListName);
    
          // Call the service to create the new list
          this.shoppingListService.createShoppingList(newListName).subscribe({
            next: (data: any) => {
              // Refresh the page
              window.location.reload();
    
              // Notify the user after a short delay
              setTimeout(() => {
                notify('New shopping list created', 'success', 2000);
              }, 500);
            },
            error: (error: any) => {
              notify('Error creating new shopping list', 'error', 2000);
              console.error('Error creating new shopping list', error);
            },
          });
        }
      },
    };
    

    // Add items for loading and syncing all lists
    const loadAllLists = {
      text: 'Sync Lists',
      icon: 'download',
      onClick: () => {
        // Handle the click event to load all lists
        this.syncAllLists();
      },
    };

    const syncAllLists = {
      text: 'Save Lists',
      icon: 'upload',
      onClick: () => {
        // Handle the click event to sync all lists
        this.storeAllLists();
      },
    };

    const deleteList = (item: any) => ({
      text: 'Delete',
      icon: 'trash',
      onClick: () => {
        // Show a confirmation message
        if (confirm(`Are you sure you want to delete the list "${item.name}"?`)) {
          // Handle the click event to delete the list
          console.log('Delete', item);
          this.shoppingListService.deleteShoppingList(item.id).subscribe({
            next: (data: any) => {
              notify('Shopping list deleted, reloading', 'success', 2000);

              // Notify the user after a short delay
              setTimeout(() => {
                window.location.reload();
              }, 1000);
            },
            error: (error: any) => {
              notify('Error deleting shopping list', 'error', 2000);
              console.error('Error deleting shopping list', error);
            },
          });
          // Adjust this based on your service
        }
      },
    });

    return [
      {
        text: 'Home',
        path: '/home',
        icon: 'home',
      },
      {
        text: 'My Lists',
        icon: 'folder',
        items: [
          ...this.menuItems.map((menuItem: any) => ({
            text: menuItem.name,
            path: '/tasks/' + menuItem.id,
            items: [
              deleteList(menuItem), // Include the delete button for each list item
            ],
          }))
        ],
      },
      {
        text: 'Manage Lists',
        icon: 'preferences',
        items: [
          createList, // Include the create list button
          loadAllLists, // Include the load all lists button
          syncAllLists, // Include the sync all lists button
        ],
      }
    ];
  }

  syncAllLists() {
    this.shoppingListService.getShoppingListsFromCloud().subscribe({
      next: (success: any) => {
        notify('Shopping lists synced with cloud', 'success', 2000);
      },
      error: (error: any) => {
        notify('Error syncing shopping lists', 'error', 2000);
        console.error('Error storing shopping lists', error);
      },
    });
  }

  storeAllLists() {
    this.shoppingListService.storeShoppingListsToCloud().subscribe({
      next: (success: any) => {
        notify('Shopping lists stored in cloud', 'success', 2000);
      },
      error: (error: any) => {
        notify('Error storing shopping lists', 'error', 2000);
        console.error('Error storing shopping lists', error);
      },
    });
  }

  ngAfterViewInit() {
    events.on(this.elementRef.nativeElement, 'dxclick', (e: Event) => {
      this.openMenu.next(e);
    });
  }

  ngOnDestroy() {
    events.off(this.elementRef.nativeElement, 'dxclick');
  }
}

@NgModule({
  imports: [ DxTreeViewModule ],
  declarations: [ SideNavigationMenuComponent ],
  exports: [ SideNavigationMenuComponent ]
})
export class SideNavigationMenuModule { }
